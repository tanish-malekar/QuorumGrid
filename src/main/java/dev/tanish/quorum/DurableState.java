package dev.tanish.quorum;

import static java.nio.file.StandardOpenOption.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32C;

/** Atomic checksummed snapshots of the full Raft term/vote/log; never acknowledge before force. */
public final class DurableState implements AutoCloseable {
  private static final int MAGIC = 0x51475231, MAX = 512 * 1024 * 1024;
  private final Path directory, file;
  private final FileChannel lockFile;
  private final FileLock lock;

  public DurableState(Path path) throws IOException {
    directory = path.toAbsolutePath();
    Files.createDirectories(directory);
    file = directory.resolve("state.bin");
    lockFile = FileChannel.open(directory.resolve("LOCK"), CREATE, WRITE);
    FileLock acquired;
    try {
      acquired = lockFile.tryLock();
      if (acquired == null) throw new IOException("Group directory already open");
    } catch (IOException | RuntimeException ex) {
      lockFile.close();
      throw ex;
    }
    lock = acquired;
    try {
      syncDirectory();
      if (directory.getParent() != null)
        try (var dir = FileChannel.open(directory.getParent(), READ)) {
          dir.force(true);
        }
    } catch (IOException | RuntimeException ex) {
      close();
      throw ex;
    }
  }

  private void syncDirectory() throws IOException {
    try (var dir = FileChannel.open(directory, READ)) {
      dir.force(true);
    }
  }

  private static void string(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String string(DataInputStream in) throws IOException {
    int size = in.readInt();
    if (size < 0 || size > 1024 * 1024) throw new IOException("Invalid string length");
    byte[] bytes = in.readNBytes(size);
    if (bytes.length != size) throw new EOFException();
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public Raft.Persistent load() throws IOException {
    if (!Files.exists(file)) return Raft.Persistent.empty();
    if (Files.size(file) > MAX || Files.size(file) < 24)
      throw new IOException("Invalid state size");
    byte[] data = Files.readAllBytes(file);
    var crc = new CRC32C();
    crc.update(data, 0, data.length - 4);
    if ((int) crc.getValue() != ByteBuffer.wrap(data, data.length - 4, 4).getInt())
      throw new IOException("State checksum mismatch");
    try (var in = new DataInputStream(new ByteArrayInputStream(data, 0, data.length - 4))) {
      if (in.readInt() != MAGIC) throw new IOException("State format mismatch");
      long term = in.readLong();
      int vote = in.readInt(), count = in.readInt();
      if (count < 1 || count > 1_000_000) throw new IOException("Invalid log length");
      var log = new ArrayList<Raft.Entry>();
      for (int i = 0; i < count; i++) {
        long entryTerm = in.readLong();
        int op = in.readUnsignedByte();
        if (op >= Raft.Op.values().length) throw new IOException("Invalid operation");
        var cmd =
            new Raft.Command(
                Raft.Op.values()[op], string(in), in.readLong(), string(in), string(in));
        if (i > 0 && cmd.op() != Raft.Op.NOOP) Raft.validate(cmd);
        log.add(new Raft.Entry(entryTerm, cmd));
      }
      if (in.available() != 0) throw new IOException("Trailing state bytes");
      return new Raft.Persistent(term, vote, log);
    }
  }

  public void save(Raft.Persistent state) {
    try {
      var bytes = new ByteArrayOutputStream();
      try (var out = new DataOutputStream(bytes)) {
        out.writeInt(MAGIC);
        out.writeLong(state.term());
        out.writeInt(state.votedFor());
        out.writeInt(state.log().size());
        for (var e : state.log()) {
          out.writeLong(e.term());
          out.writeByte(e.command().op().ordinal());
          string(out, e.command().client());
          out.writeLong(e.command().sequence());
          string(out, e.command().key());
          string(out, e.command().value());
        }
      }
      byte[] payload = bytes.toByteArray();
      if (payload.length > MAX - 4) throw new IOException("State file capacity reached");
      var crc = new CRC32C();
      crc.update(payload);
      var frame = ByteBuffer.allocate(payload.length + 4).put(payload).putInt((int) crc.getValue());
      frame.flip();
      Path temp = directory.resolve("state.tmp");
      try (var channel = FileChannel.open(temp, CREATE, TRUNCATE_EXISTING, WRITE)) {
        while (frame.hasRemaining()) channel.write(frame);
        channel.force(true);
      }
      Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      syncDirectory();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      lock.release();
    } finally {
      lockFile.close();
    }
  }
}
