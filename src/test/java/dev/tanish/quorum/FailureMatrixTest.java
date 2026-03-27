package dev.tanish.quorum;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.stream.*;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

class FailureMatrixTest {
  static final String[] NAMES = {
    "leader-crash",
    "follower-crash",
    "two-follower-crashes",
    "leader-partition",
    "follower-partition",
    "minority-partition",
    "request-loss",
    "response-loss",
    "bidirectional-loss",
    "leader-restart",
    "follower-restart",
    "rolling-restart",
    "asymmetric-partition",
    "divergent-suffix",
    "retry-deduplication",
    "crash-plus-partition"
  };

  static Stream<Arguments> cases() {
    return IntStream.range(0, 3)
        .boxed()
        .flatMap(g -> IntStream.range(0, 16).mapToObj(f -> Arguments.of(g, f, NAMES[f])));
  }

  static final class Simulation {
    long time, messages;
    int requestLoss, responseLoss;
    final int group;
    final Raft.Persistent[] disk = new Raft.Persistent[5];
    final Raft[] nodes = new Raft[5];
    final boolean[][] blocked = new boolean[5][5];

    Simulation(int group) {
      this.group = group;
      for (int n = 0; n < 5; n++) {
        disk[n] = Raft.Persistent.empty();
        restart(n);
      }
    }

    void restart(int n) {
      nodes[n] =
          new Raft(n, disk[n], state -> disk[n] = state, 1001 + n * 37 + group * 101 + time, time);
    }

    void crash(int n) {
      nodes[n] = null;
    }

    void isolate(int n) {
      for (int p = 0; p < 5; p++) if (p != n) blocked[n][p] = blocked[p][n] = true;
    }

    void heal() {
      for (var row : blocked) Arrays.fill(row, false);
      requestLoss = responseLoss = 0;
    }

    void step() {
      time++;
      for (var node : nodes) if (node != null) node.tick(time);
      for (int from = 0; from < 5; from++)
        if (nodes[from] != null)
          for (int to = 0; to < 5; to++)
            if (from != to) {
              var work = nodes[from].take(to, time);
              if (work == null) continue;
              messages++;
              if (nodes[to] == null
                  || blocked[from][to]
                  || (requestLoss > 0 && messages % requestLoss == 0)) continue;
              if (work.vote() != null) {
                var reply = nodes[to].vote(work.vote(), time);
                if (!blocked[to][from] && !(responseLoss > 0 && messages % responseLoss == 0))
                  nodes[from].reply(to, work, reply, time);
              } else {
                var reply = nodes[to].append(work.append(), time);
                if (!blocked[to][from] && !(responseLoss > 0 && messages % responseLoss == 0))
                  nodes[from].reply(to, work, reply, time);
              }
            }
    }

    void run(int count) {
      for (int i = 0; i < count; i++) step();
    }

    int leader() {
      int best = -1;
      for (int n = 0; n < 5; n++)
        if (nodes[n] != null
            && nodes[n].role() == Raft.Role.LEADER
            && (best < 0 || nodes[n].term() > nodes[best].term())) best = n;
      return best;
    }

    Raft.Result execute(Raft.Command command) {
      int target = -1, index = -1;
      long term = -1;
      for (int elapsed = 0; elapsed < 4000; elapsed++) {
        int selected = leader();
        if (selected >= 0 && (selected != target || nodes[selected].term() != term || index < 0)) {
          target = selected;
          term = nodes[target].term();
          index = nodes[target].propose(command);
        }
        if (target >= 0
            && nodes[target] != null
            && nodes[target].term() == term
            && nodes[target].role() == Raft.Role.LEADER
            && index >= 0) {
          var result = nodes[target].result(index);
          if (result != null) return result;
        }
        step();
      }
      fail("No committed result within virtual-time budget");
      return null;
    }
  }

  static Raft.Command command(Raft.Op op, String client, long seq, String key, String value) {
    return new Raft.Command(op, client, seq, key, value);
  }

  @ParameterizedTest(name = "group {0}: {2}")
  @MethodSource("cases")
  void committedStateSurvivesAndMinorityCannotRead(
      int group, int fault, String name, TestReporter report) {
    var sim = new Simulation(group);
    sim.run(500);
    int original = sim.leader();
    assertTrue(original >= 0);
    String key = "register";
    while (Raft.shard(key) != group) key += "x";
    assertEquals("", sim.execute(command(Raft.Op.PUT, "writer", 1, key, "committed")).error());
    sim.run(60);
    int follower = (original + 1) % 5, second = (original + 2) % 5;
    long started = sim.time;
    switch (fault) {
      case 0, 14 -> sim.crash(original);
      case 1 -> sim.crash(follower);
      case 2 -> {
        sim.crash(follower);
        sim.crash(second);
      }
      case 3 -> sim.isolate(original);
      case 4 -> sim.isolate(follower);
      case 5 -> {
        for (int a : new int[] {original, follower})
          for (int b = 0; b < 5; b++)
            if (b != original && b != follower) sim.blocked[a][b] = sim.blocked[b][a] = true;
      }
      case 6 -> sim.requestLoss = 3;
      case 7 -> sim.responseLoss = 3;
      case 8 -> {
        sim.requestLoss = 4;
        sim.responseLoss = 5;
      }
      case 9 -> {
        sim.crash(original);
        sim.run(400);
        sim.restart(original);
      }
      case 10 -> {
        sim.crash(follower);
        sim.run(100);
        sim.restart(follower);
      }
      case 11 -> {
        sim.crash(follower);
        sim.crash(second);
        sim.run(200);
        sim.restart(follower);
      }
      case 12 -> {
        for (int p = 0; p < 5; p++) if (p != original) sim.blocked[original][p] = true;
      }
      case 13 -> {
        sim.isolate(original);
        assertTrue(
            sim.nodes[original].propose(command(Raft.Op.PUT, "unacknowledged", 1, key, "ghost"))
                > 0);
      }
      case 15 -> {
        sim.crash(follower);
        sim.isolate(original);
      }
      default -> throw new AssertionError();
    }
    sim.run(600);
    assertTrue(sim.leader() >= 0);
    report.publishEntry("virtual_settle_ms", Long.toString(sim.time - started));
    if (Set.of(3, 5, 13, 15).contains(fault) && sim.nodes[original].role() == Raft.Role.LEADER) {
      int read = sim.nodes[original].propose(command(Raft.Op.GET, "isolated-reader", 1, key, ""));
      sim.run(250);
      assertNull(sim.nodes[original].result(read));
    }
    var observed = sim.execute(command(Raft.Op.GET, "reader", 1, key, ""));
    assertTrue(observed.found());
    assertEquals("committed", observed.value());
    assertEquals("", observed.error());
    var updated = sim.execute(command(Raft.Op.PUT, "writer", 2, key, "latest"));
    assertEquals("", updated.error());
    if (fault == 14) {
      assertEquals("latest", sim.execute(command(Raft.Op.PUT, "writer", 2, key, "latest")).value());
      assertFalse(sim.execute(command(Raft.Op.PUT, "writer", 2, key, "wrong")).error().isEmpty());
    }
    sim.heal();
    for (int n = 0; n < 5; n++) if (sim.nodes[n] == null) sim.restart(n);
    sim.run(1000);
    assertEquals("latest", sim.execute(command(Raft.Op.GET, "reader", 2, key, "")).value());
    for (int n = 0; n < 5; n++) {
      sim.crash(n);
      sim.restart(n);
      sim.run(200);
    }
    assertEquals("latest", sim.execute(command(Raft.Op.GET, "reader", 3, key, "")).value());
  }
}
