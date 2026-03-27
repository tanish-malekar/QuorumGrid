package dev.tanish.quorum;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableStateTest {
  @TempDir Path root;

  @Test
  void termVoteAndLogSurviveReopen() throws Exception {
    var state =
        new Raft.Persistent(
            4,
            2,
            List.of(
                new Raft.Entry(0, Raft.Command.noop()),
                new Raft.Entry(4, new Raft.Command(Raft.Op.PUT, "client", 1, "key", "hello"))));
    try (var store = new DurableState(root)) {
      store.save(state);
    }
    try (var store = new DurableState(root)) {
      assertEquals(state, store.load());
    }
  }

  @Test
  void corruptionIsRejectedAndUnrenamedTempIsIgnored() throws Exception {
    try (var store = new DurableState(root)) {
      store.save(Raft.Persistent.empty());
    }
    Files.writeString(root.resolve("state.tmp"), "incomplete write");
    try (var store = new DurableState(root)) {
      assertEquals(0, store.load().term());
    }
    byte[] bytes = Files.readAllBytes(root.resolve("state.bin"));
    bytes[8] ^= 1;
    Files.write(root.resolve("state.bin"), bytes);
    try (var store = new DurableState(root)) {
      assertThrows(java.io.IOException.class, store::load);
    }
  }

  @Test
  void voteIsForcedBeforeGrantAndCannotBeCastTwice() {
    var durable = new ArrayList<Raft.Persistent>();
    var node = new Raft(0, Raft.Persistent.empty(), durable::add, 7, 0);
    assertTrue(node.vote(new Raft.Vote(1, 2, 0, 0), 1).granted());
    assertEquals(2, durable.getLast().votedFor());
    var restarted = new Raft(0, durable.getLast(), durable::add, 8, 2);
    assertFalse(restarted.vote(new Raft.Vote(1, 3, 0, 0), 3).granted());
  }
}
