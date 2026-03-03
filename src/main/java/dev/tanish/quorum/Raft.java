package dev.tanish.quorum;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

/** Single-threaded deterministic state machine; caller serializes each group's access. */
public final class Raft {
  public enum Op {
    NOOP,
    PUT,
    GET,
    ERASE
  }

  public enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER
  }

  public record Command(Op op, String client, long sequence, String key, String value) {
    public static Command noop() {
      return new Command(Op.NOOP, "", 0, "", "");
    }
  }

  public record Entry(long term, Command command) {}

  public record Persistent(long term, int votedFor, List<Entry> log) {
    public Persistent {
      log = List.copyOf(log);
    }

    public static Persistent empty() {
      return new Persistent(0, -1, List.of(new Entry(0, Command.noop())));
    }
  }

  public record Result(boolean found, String value, String error) {}

  public record Vote(long term, int candidate, int lastIndex, long lastTerm) {}

  public record Voted(long term, boolean granted) {}

  public record Append(
      long term,
      int leader,
      int previousIndex,
      long previousTerm,
      int commit,
      List<Entry> entries) {
    public Append {
      entries = List.copyOf(entries);
    }
  }

  public record Appended(long term, boolean success, int match, int hint) {}

  public record Work(Vote vote, Append append) {}

  private record Session(Command command, Result result) {}

  private final int id;
  private final Consumer<Persistent> save;
  private final Random random;
  private final ArrayList<Entry> log;
  private final int[] next = new int[5], matched = new int[5];
  private final boolean[] votes = new boolean[5], sentVote = new boolean[5];
  private final long[] heartbeat = new long[5];
  private final Map<String, String> values = new HashMap<>();
  private final Map<String, Session> sessions = new HashMap<>();
  private final Map<Integer, Result> results = new HashMap<>();
  private long term, deadline;
  private int votedFor, leader = -1, commit, applied;
  private Role role = Role.FOLLOWER;

  public Raft(int id, Persistent state, Consumer<Persistent> save, long seed, long now) {
    if (id < 0
        || id >= 5
        || state.term() < 0
        || state.votedFor() < -1
        || state.votedFor() >= 5
        || state.log().isEmpty()
        || state.log().getFirst().term() != 0)
      throw new IllegalArgumentException("Invalid five-member Raft state");
    this.id = id;
    this.save = save;
    random = new Random(seed);
    term = state.term();
    votedFor = state.votedFor();
    log = new ArrayList<>(state.log());
    for (int i = 1; i < log.size(); i++)
      if (log.get(i).term() < log.get(i - 1).term() || log.get(i).term() > term)
        throw new IllegalArgumentException("Invalid term order");
    Arrays.fill(next, 1);
    reset(now);
  }

  public static int shard(String key) {
    long hash = 0xcbf29ce484222325L;
    for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
      hash ^= b & 255;
      hash *= 0x100000001b3L;
    }
    return (int) Long.remainderUnsigned(hash, 3);
  }

  public Persistent persistent() {
    return new Persistent(term, votedFor, log);
  }

  private void persist() {
    save.accept(persistent());
  }

  private void reset(long now) {
    deadline = now + 80 + random.nextInt(81);
  }

  private void follower(long newer, long now) {
    if (newer > term) {
      term = newer;
      votedFor = -1;
      persist();
    }
    role = Role.FOLLOWER;
    leader = -1;
    reset(now);
  }

  public void tick(long now) {
    if (role == Role.LEADER || now < deadline) return;
    role = Role.CANDIDATE;
    leader = -1;
    term = Math.incrementExact(term);
    votedFor = id;
    Arrays.fill(votes, false);
    Arrays.fill(sentVote, false);
    votes[id] = true;
    persist();
    reset(now);
  }

  private void becomeLeader(long now) {
    role = Role.LEADER;
    leader = id;
    Arrays.fill(next, log.size());
    Arrays.fill(matched, 0);
    Arrays.fill(heartbeat, now);
    log.add(new Entry(term, Command.noop()));
    persist();
    matched[id] = lastIndex();
    advance();
  }

  public Voted vote(Vote r, long now) {
    if (r.candidate() < 0 || r.candidate() >= 5 || r.lastIndex() < 0) return new Voted(term, false);
    if (r.term() > term) follower(r.term(), now);
    long last = log.getLast().term();
    boolean fresh = r.lastTerm() > last || (r.lastTerm() == last && r.lastIndex() >= lastIndex());
    boolean granted = r.term() == term && fresh && (votedFor == -1 || votedFor == r.candidate());
    if (granted) {
      votedFor = r.candidate();
      persist();
      reset(now);
    }
    return new Voted(term, granted);
  }

  public Appended append(Append r, long now) {
    if (r.leader() < 0
        || r.leader() >= 5
        || r.term() < term
        || r.previousIndex() < 0
        || r.commit() < 0) return new Appended(term, false, 0, log.size());
    if (r.term() > term) follower(r.term(), now);
    role = Role.FOLLOWER;
    leader = r.leader();
    reset(now);
    if (r.previousIndex() > lastIndex()) return new Appended(term, false, 0, log.size());
    if (log.get(r.previousIndex()).term() != r.previousTerm()) {
      int first = r.previousIndex();
      long conflict = log.get(first).term();
      while (first > 0 && log.get(first - 1).term() == conflict) first--;
      return new Appended(term, false, 0, first);
    }
    int at = r.previousIndex() + 1;
    boolean changed = false;
    for (Entry entry : r.entries()) {
      if (entry.term() <= 0 || entry.term() > r.term() || entry.term() < log.get(at - 1).term())
        throw new IllegalArgumentException("Malformed append term");
      if (at < log.size() && log.get(at).term() != entry.term()) {
        if (at <= commit) throw new IllegalStateException("Committed overwrite");
        log.subList(at, log.size()).clear();
        changed = true;
      }
      if (at == log.size()) {
        log.add(entry);
        changed = true;
      } else if (!log.get(at).equals(entry))
        throw new IllegalStateException("Same-term command mismatch");
      at++;
    }
    if (changed) persist();
    int confirmed = r.previousIndex() + r.entries().size();
    int updated = Math.min(r.commit(), confirmed);
    if (updated > commit) {
      commit = updated;
      apply();
    }
    return new Appended(term, true, confirmed, confirmed + 1);
  }

  public Work take(int peer, long now) {
    if (peer < 0 || peer >= 5 || peer == id) return null;
    if (role == Role.CANDIDATE && !sentVote[peer]) {
      sentVote[peer] = true;
      return new Work(new Vote(term, id, lastIndex(), log.getLast().term()), null);
    }
    if (role != Role.LEADER || (now < heartbeat[peer] && next[peer] > lastIndex())) return null;
    heartbeat[peer] = now + 20;
    int start = Math.max(1, next[peer]);
    List<Entry> entries = new ArrayList<>();
    int bytes = 0;
    for (int i = start; i < log.size() && entries.size() < 64; i++) {
      Command c = log.get(i).command();
      int size =
          c.key().getBytes(StandardCharsets.UTF_8).length
              + c.value().getBytes(StandardCharsets.UTF_8).length
              + 256;
      if (bytes + size > 2 * 1024 * 1024 && !entries.isEmpty()) break;
      entries.add(log.get(i));
      bytes += size;
    }
    return new Work(
        null, new Append(term, id, start - 1, log.get(start - 1).term(), commit, entries));
  }

  public void reply(int peer, Work work, Voted r, long now) {
    if (r.term() > term) {
      follower(r.term(), now);
      return;
    }
    if (work.vote() == null
        || role != Role.CANDIDATE
        || work.vote().term() != term
        || r.term() != term) return;
    if (r.granted()) votes[peer] = true;
    int count = 0;
    for (boolean vote : votes) if (vote) count++;
    if (count >= 3) becomeLeader(now);
  }

  public void reply(int peer, Work work, Appended r, long now) {
    if (r.term() > term) {
      follower(r.term(), now);
      return;
    }
    if (work.append() == null
        || role != Role.LEADER
        || work.append().term() != term
        || r.term() != term) return;
    if (r.success()) {
      int expected = work.append().previousIndex() + work.append().entries().size();
      if (r.match() != expected) return;
      matched[peer] = Math.max(matched[peer], r.match());
      next[peer] = matched[peer] + 1;
      advance();
    } else
      next[peer] = Math.max(matched[peer] + 1, Math.max(1, Math.min(next[peer] - 1, r.hint())));
  }

  private void advance() {
    for (int n = lastIndex(); n > commit; n--) {
      if (log.get(n).term() != term) continue;
      int count = 1;
      for (int p = 0; p < 5; p++) if (p != id && matched[p] >= n) count++;
      if (count >= 3) {
        commit = n;
        apply();
        break;
      }
    }
  }

  private void apply() {
    while (applied < commit) {
      int index = ++applied;
      Command cmd = log.get(index).command();
      Result result = new Result(false, "", "");
      if (cmd.op() != Op.NOOP) {
        Session cached = sessions.get(cmd.client());
        if (cached != null && cmd.sequence() <= cached.command().sequence()) {
          result =
              cmd.equals(cached.command())
                  ? cached.result()
                  : new Result(false, "", "Stale or reused client sequence");
        } else {
          boolean found = values.containsKey(cmd.key());
          String value = values.getOrDefault(cmd.key(), "");
          if (cmd.op() == Op.PUT) {
            values.put(cmd.key(), cmd.value());
            found = true;
            value = cmd.value();
          } else if (cmd.op() == Op.ERASE) values.remove(cmd.key());
          result = new Result(found, value, "");
          sessions.put(cmd.client(), new Session(cmd, result));
        }
      }
      results.put(index, result);
    }
  }

  public int propose(Command cmd) {
    if (role != Role.LEADER) return -1;
    validate(cmd);
    if (log.size() >= 1_000_000) throw new IllegalStateException("Log capacity reached");
    log.add(new Entry(term, cmd));
    persist();
    matched[id] = lastIndex();
    advance();
    return lastIndex();
  }

  public static void validate(Command c) {
    if (c.op() == Op.NOOP
        || c.client().isBlank()
        || c.client().length() > 128
        || c.sequence() <= 0
        || c.key().isEmpty()
        || c.key().getBytes(StandardCharsets.UTF_8).length > 1024
        || c.value().getBytes(StandardCharsets.UTF_8).length > 1024 * 1024)
      throw new IllegalArgumentException("Invalid client command");
  }

  public Result result(int index) {
    return results.get(index);
  }

  public Role role() {
    return role;
  }

  public int leader() {
    return leader;
  }

  public long term() {
    return term;
  }

  public int committed() {
    return commit;
  }

  public int lastIndex() {
    return log.size() - 1;
  }
}
