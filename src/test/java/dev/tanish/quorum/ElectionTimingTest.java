package dev.tanish.quorum;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestReporter;

class ElectionTimingTest {
  @Test
  void reportFirstElectionAndLeaderReplacementInVirtualMilliseconds(TestReporter report) {
    var sim = new FailureMatrixTest.Simulation(0);
    while (sim.leader() < 0 && sim.time < 1000) sim.step();
    assertTrue(sim.leader() >= 0);
    report.publishEntry("initial_election_virtual_ms", Long.toString(sim.time));
    int old = sim.leader();
    long began = sim.time;
    sim.crash(old);
    while (sim.leader() < 0 && sim.time - began < 2000) sim.step();
    assertTrue(sim.leader() >= 0);
    report.publishEntry("replacement_election_virtual_ms", Long.toString(sim.time - began));
  }
}
