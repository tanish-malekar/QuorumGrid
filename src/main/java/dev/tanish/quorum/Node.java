package dev.tanish.quorum;

import dev.tanish.quorum.wire.*;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Three independent consensus groups hosted in one of five loopback lab processes. */
public final class Node implements AutoCloseable {
  private static long now() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
  }

  private static final class Group {
    final DurableState store;
    final Raft raft;
    boolean healthy = true;

    Group(Path path, int id, long seed) throws IOException {
      store = new DurableState(path);
      try {
        raft = new Raft(id, store.load(), store::save, seed, now());
      } catch (IOException | RuntimeException ex) {
        store.close();
        throw ex;
      }
    }
  }

  private final int id;
  private final Path faults;
  private final Group[] groups = new Group[3];
  private final ManagedChannel[] channels = new ManagedChannel[5];
  private final ClusterGrpc.ClusterBlockingStub[] peers = new ClusterGrpc.ClusterBlockingStub[5];
  private final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
  private final ScheduledExecutorService transport = Executors.newScheduledThreadPool(12);
  private final ExecutorService handlers = Executors.newFixedThreadPool(24);
  private Server server;

  public Node(int id, Path data, int basePort, Path faults) throws IOException {
    if (id < 0 || id >= 5 || basePort < 1024 || basePort > 65530)
      throw new IllegalArgumentException("Invalid node/port");
    this.id = id;
    this.faults = faults;
    try {
      for (int g = 0; g < 3; g++)
        groups[g] = new Group(data.resolve("group-" + g), id, 991L + id * 47L + g * 1009L);
      for (int p = 0; p < 5; p++)
        if (p != id) {
          channels[p] =
              ManagedChannelBuilder.forAddress("127.0.0.1", basePort + p)
                  .usePlaintext()
                  .maxInboundMessageSize(4 * 1024 * 1024)
                  .build();
          peers[p] = ClusterGrpc.newBlockingStub(channels[p]);
        }
      server =
          NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", basePort + id))
              .executor(handlers)
              .maxInboundMessageSize(4 * 1024 * 1024)
              .addService(new Service())
              .build()
              .start();
      ticker.scheduleWithFixedDelay(
          () -> {
            for (Group group : groups)
              synchronized (group) {
                if (!group.healthy) continue;
                try {
                  group.raft.tick(now());
                  group.notifyAll();
                } catch (RuntimeException ex) {
                  fence(group, ex);
                }
              }
          },
          0,
          5,
          TimeUnit.MILLISECONDS);
      for (int g = 0; g < 3; g++)
        for (int p = 0; p < 5; p++)
          if (p != id) {
            final int shard = g, peer = p;
            transport.scheduleWithFixedDelay(
                () -> exchange(shard, peer), 0, 3, TimeUnit.MILLISECONDS);
          }
    } catch (IOException | RuntimeException ex) {
      close();
      throw ex;
    }
  }

  private static void fence(Group group, Exception ex) {
    group.healthy = false;
    group.notifyAll();
    System.err.println("Group fenced: " + ex);
  }

  private boolean dropped(int group, int peer) {
    if (faults == null || !Files.exists(faults)) return false;
    try {
      for (String line : Files.readAllLines(faults)) {
        line = line.strip();
        if (line.isEmpty() || line.startsWith("#")) continue;
        String[] words = line.split("\\s+");
        if (words.length != 2) throw new IOException("Invalid fault rule");
        int g = Integer.parseInt(words[0]), p = Integer.parseInt(words[1]);
        if ((g == -1 || g == group) && p == peer) return true;
      }
      return false;
    } catch (IOException | NumberFormatException ex) {
      return true;
    }
  }

  private static int index(long value) {
    if (value < 0 || value > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Index outside capacity");
    return (int) value;
  }

  private static void term(long value) {
    if (value < 0) throw new IllegalArgumentException("Term exceeds signed range");
  }

  static Command encode(Raft.Command c) {
    return Command.newBuilder()
        .setOperation(Operation.valueOf(c.op().name()))
        .setClientId(c.client())
        .setSequence(c.sequence())
        .setKey(c.key())
        .setValue(c.value())
        .build();
  }

  static Raft.Command decode(Command c) {
    if (c.getOperation() == Operation.UNRECOGNIZED)
      throw new IllegalArgumentException("Unknown operation");
    return new Raft.Command(
        Raft.Op.valueOf(c.getOperation().name()),
        c.getClientId(),
        c.getSequence(),
        c.getKey(),
        c.getValue());
  }

  private void exchange(int shard, int peer) {
    if (dropped(shard, peer)) return;
    Group group = groups[shard];
    Raft.Work work;
    synchronized (group) {
      if (!group.healthy) return;
      try {
        work = group.raft.take(peer, now());
      } catch (RuntimeException ex) {
        fence(group, ex);
        return;
      }
    }
    if (work == null) return;
    var envelope = Envelope.newBuilder().setGroup(shard).setFrom(id);
    if (work.vote() != null) {
      var v = work.vote();
      envelope.setVote(
          VoteRequest.newBuilder()
              .setTerm(v.term())
              .setCandidate(v.candidate())
              .setLastIndex(v.lastIndex())
              .setLastTerm(v.lastTerm()));
    } else {
      var a = work.append();
      var wire =
          AppendRequest.newBuilder()
              .setTerm(a.term())
              .setLeader(a.leader())
              .setPreviousIndex(a.previousIndex())
              .setPreviousTerm(a.previousTerm())
              .setLeaderCommit(a.commit());
      for (var e : a.entries())
        wire.addEntries(LogEntry.newBuilder().setTerm(e.term()).setCommand(encode(e.command())));
      envelope.setAppend(wire);
    }
    PeerReply reply;
    try {
      reply = peers[peer].withDeadlineAfter(30, TimeUnit.MILLISECONDS).exchange(envelope.build());
    } catch (StatusRuntimeException ex) {
      return;
    }
    if (dropped(shard, peer)) return;
    synchronized (group) {
      if (!group.healthy) return;
      try {
        if (work.vote() != null && reply.hasVote()) {
          term(reply.getVote().getTerm());
          group.raft.reply(
              peer,
              work,
              new Raft.Voted(reply.getVote().getTerm(), reply.getVote().getGranted()),
              now());
        } else if (work.append() != null && reply.hasAppend()) {
          var a = reply.getAppend();
          term(a.getTerm());
          group.raft.reply(
              peer,
              work,
              new Raft.Appended(
                  a.getTerm(), a.getSuccess(), index(a.getMatch()), index(a.getHint())),
              now());
        }
        group.notifyAll();
      } catch (RuntimeException ex) {
        fence(group, ex);
      }
    }
  }

  private Group group(int number) {
    if (number < 0 || number >= 3) throw new IllegalArgumentException("Unknown group");
    return groups[number];
  }

  private static <T> void send(StreamObserver<T> observer, T value) {
    observer.onNext(value);
    observer.onCompleted();
  }

  private final class Service extends ClusterGrpc.ClusterImplBase {
    @Override
    public void exchange(Envelope request, StreamObserver<PeerReply> observer) {
      try {
        int shard = request.getGroup(), from = request.getFrom();
        Group group = group(shard);
        if (from < 0 || from >= 5 || from == id)
          throw new IllegalArgumentException("Invalid sender");
        if (dropped(shard, from)) {
          observer.onError(Status.UNAVAILABLE.asRuntimeException());
          return;
        }
        Raft.Vote vote = null;
        Raft.Append append = null;
        if (request.hasVote()) {
          var v = request.getVote();
          term(v.getTerm());
          term(v.getLastTerm());
          if (v.getCandidate() != from) throw new IllegalArgumentException("Sender mismatch");
          vote = new Raft.Vote(v.getTerm(), from, index(v.getLastIndex()), v.getLastTerm());
        } else if (request.hasAppend()) {
          var a = request.getAppend();
          term(a.getTerm());
          term(a.getPreviousTerm());
          if (a.getLeader() != from || a.getEntriesCount() > 64)
            throw new IllegalArgumentException("Invalid append envelope");
          var entries = new ArrayList<Raft.Entry>();
          for (var e : a.getEntriesList()) {
            term(e.getTerm());
            var cmd = decode(e.getCommand());
            if (cmd.op() != Raft.Op.NOOP) Raft.validate(cmd);
            entries.add(new Raft.Entry(e.getTerm(), cmd));
          }
          append =
              new Raft.Append(
                  a.getTerm(),
                  from,
                  index(a.getPreviousIndex()),
                  a.getPreviousTerm(),
                  index(a.getLeaderCommit()),
                  entries);
        } else throw new IllegalArgumentException("Missing request");
        PeerReply response;
        synchronized (group) {
          if (!group.healthy) {
            observer.onError(Status.UNAVAILABLE.asRuntimeException());
            return;
          }
          try {
            if (vote != null) {
              var result = group.raft.vote(vote, now());
              response =
                  PeerReply.newBuilder()
                      .setVote(
                          VoteReply.newBuilder()
                              .setTerm(result.term())
                              .setGranted(result.granted()))
                      .build();
            } else {
              var result = group.raft.append(append, now());
              response =
                  PeerReply.newBuilder()
                      .setAppend(
                          AppendReply.newBuilder()
                              .setTerm(result.term())
                              .setSuccess(result.success())
                              .setMatch(result.match())
                              .setHint(result.hint()))
                      .build();
            }
            group.notifyAll();
          } catch (RuntimeException ex) {
            fence(group, ex);
            throw ex;
          }
        }
        if (dropped(shard, from)) observer.onError(Status.UNAVAILABLE.asRuntimeException());
        else send(observer, response);
      } catch (IllegalArgumentException ex) {
        observer.onError(
            Status.INVALID_ARGUMENT.withDescription(ex.getMessage()).asRuntimeException());
      } catch (RuntimeException ex) {
        observer.onError(
            Status.UNAVAILABLE.withDescription("Group requires restart").asRuntimeException());
      }
    }

    @Override
    public void execute(ClientRequest request, StreamObserver<ClientReply> observer) {
      try {
        Group group = group(request.getGroup());
        var command = decode(request.getCommand());
        Raft.validate(command);
        if (Raft.shard(command.key()) != request.getGroup())
          throw new IllegalArgumentException("Wrong shard");
        ClientReply response;
        synchronized (group) {
          if (!group.healthy) {
            observer.onError(Status.UNAVAILABLE.asRuntimeException());
            return;
          }
          if (group.raft.role() != Raft.Role.LEADER) {
            response =
                ClientReply.newBuilder()
                    .setCode(ClientReply.Code.NOT_LEADER)
                    .setLeader(group.raft.leader())
                    .build();
          } else {
            long currentTerm = group.raft.term();
            int index;
            try {
              index = group.raft.propose(command);
            } catch (RuntimeException ex) {
              fence(group, ex);
              throw ex;
            }
            long deadline = now() + 2000;
            while (group.healthy
                && group.raft.role() == Raft.Role.LEADER
                && group.raft.term() == currentTerm
                && group.raft.result(index) == null
                && now() < deadline
                && !Context.current().isCancelled()) group.wait(10);
            var result = group.raft.result(index);
            var builder =
                ClientReply.newBuilder()
                    .setLeader(group.raft.leader())
                    .setTerm(currentTerm)
                    .setIndex(index);
            if (group.healthy
                && group.raft.role() == Raft.Role.LEADER
                && group.raft.term() == currentTerm
                && result != null)
              response =
                  builder
                      .setCode(
                          result.error().isEmpty()
                              ? ClientReply.Code.OK
                              : ClientReply.Code.REJECTED)
                      .setFound(result.found())
                      .setValue(result.value())
                      .setError(result.error())
                      .build();
            else response = builder.setCode(ClientReply.Code.UNKNOWN_OUTCOME).build();
          }
        }
        send(observer, response);
      } catch (IllegalArgumentException ex) {
        observer.onError(
            Status.INVALID_ARGUMENT.withDescription(ex.getMessage()).asRuntimeException());
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        observer.onError(Status.CANCELLED.asRuntimeException());
      } catch (RuntimeException ex) {
        observer.onError(Status.UNAVAILABLE.asRuntimeException());
      }
    }

    @Override
    public void status(StatusRequest request, StreamObserver<StatusReply> observer) {
      var result = StatusReply.newBuilder();
      for (int g = 0; g < 3; g++) {
        var group = groups[g];
        synchronized (group) {
          result.addGroups(
              GroupStatus.newBuilder()
                  .setGroup(g)
                  .setRole(group.raft.role().name())
                  .setTerm(group.raft.term())
                  .setLeader(group.raft.leader())
                  .setCommitted(group.raft.committed())
                  .setHealthy(group.healthy));
        }
      }
      send(observer, result.build());
    }
  }

  public void await() throws InterruptedException {
    server.awaitTermination();
  }

  @Override
  public void close() throws IOException {
    for (Group group : groups)
      if (group != null)
        synchronized (group) {
          group.healthy = false;
          group.notifyAll();
        }
    if (server != null) server.shutdownNow();
    ticker.shutdownNow();
    transport.shutdownNow();
    handlers.shutdownNow();
    for (var channel : channels) if (channel != null) channel.shutdownNow();
    try {
      boolean stopped =
          ticker.awaitTermination(5, TimeUnit.SECONDS)
              && transport.awaitTermination(5, TimeUnit.SECONDS)
              && handlers.awaitTermination(5, TimeUnit.SECONDS);
      if (stopped) for (Group group : groups) if (group != null) group.store.close();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException("Node ID DATA_DIRECTORY [BASE_PORT=52051] [FAULT_FILE]");
    var node =
        new Node(
            Integer.parseInt(args[0]),
            Path.of(args[1]),
            args.length > 2 ? Integer.parseInt(args[2]) : 52051,
            args.length > 3 ? Path.of(args[3]) : null);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    node.close();
                  } catch (IOException ex) {
                    System.err.println(ex);
                  }
                }));
    node.await();
  }
}
