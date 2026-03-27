# QuorumGrid

QuorumGrid is a Java 21 key-value store with three independent Raft consensus
groups hosted on five processes. UTF-8 keys are assigned with unsigned FNV-1a
modulo three. Each group has its own leader, term, vote, log, commit index, and
state machine; all five processes are voters in every group. Protobuf and gRPC
carry vote requests, append requests, client operations, and status information.

## Consensus and command semantics

The Raft core implements randomized elections, one durable vote per term,
log-freshness checks, conflict hints, suffix repair, current-term majority commit,
and ordered state-machine application. Leaders append a no-op barrier when elected.
Election timers range from 80–160 ms with 20 ms heartbeat scheduling and 30 ms peer
RPC deadlines. These are configuration constants, not wall-clock performance
measurements; JVM scheduling, disk forcing, and transport delay affect elections.

`PUT`, `GET`, and `ERASE` all enter the replicated log. A leader only returns a
result after majority replication and local application while retaining its term
and role. An isolated old leader cannot complete reads. Linearizability applies
to single-shard operations under Raft's crash-fault assumptions; the system has no
cross-shard transaction or multi-key atomicity.

Clients send stable IDs and increasing sequence numbers. The replicated state
machine remembers each client's most recent request/result in each group, replays
identical retries, and rejects stale or reused sequence numbers. Use one outstanding
operation per client ID per group. Timeouts mean an unknown outcome: retry the same
request identity instead of assuming that the write failed.

## Durability and concurrency

`DurableState` encodes term, vote, and the full log in a CRC32C-protected file. It
forces a replacement file, atomically renames it, and forces the directory before
voting or acknowledging replication. Each group directory has an exclusive lock.
On restart, volatile values and sessions rebuild by applying the retained log after
a leader's current-term commit barrier. Persist or invariant failures fence the
group until restart. Local POSIX filesystems must honor atomic rename and fsync.

A separate lock guards each group's core. Twelve peer workers perform outbound
RPCs outside those locks. Three groups share one process but do not share consensus
state. A ticker drives elections; gRPC handlers accept inbound requests. The
implementation bounds keys to 1 KiB, values to 1 MiB, append batches to 64 entries
and roughly 2 MiB, state files to 512 MiB, and logs to one million entries. It retains
full logs and results, rewrites durable state on changes, and has fixed membership;
it does not implement snapshots, membership changes, or shard migration.

## Build and start

Requires Java 21, Maven 3.9+, and Python 3 for the local launcher. Maven generates
Java protobuf messages and gRPC stubs from `src/main/proto/cluster.proto`.

```sh
mvn -DskipTests package
python3 tools/cluster.py
```

Five loopback servers listen on ports 52051–52055. The launcher writes per-node
logs and PIDs under `cluster-data/`; Ctrl-C stops its child processes while keeping
the durable directories. To restart a killed node manually:

```sh
java -cp target/quorumgrid-1.0.0.jar dev.tanish.quorum.Node \
  0 cluster-data/node-0 52051 cluster-data/node-0/faults.txt
```

From another terminal:

```sh
java -cp target/quorumgrid-1.0.0.jar dev.tanish.quorum.Client 52051 demo 1 PUT customer:1 Ada
java -cp target/quorumgrid-1.0.0.jar dev.tanish.quorum.Client 52051 demo 2 GET customer:1
java -cp target/quorumgrid-1.0.0.jar dev.tanish.quorum.Client 52051 demo 3 ERASE customer:1
```

The transport binds loopback, uses plaintext, and has no authentication. It is a
local distributed-systems lab. Remote deployments require authenticated peer and
client transport plus explicit membership configuration.

## Fault exercises and JUnit

```sh
mvn test
```

`FailureMatrixTest` generates 48 named cases: 16 fault scenarios across each of the
three groups. Its deterministic virtual-time network models process crashes,
restarts with saved term/vote/log state, majority/minority partitions, asymmetric
partitions, request loss, response loss, divergent uncommitted suffixes, and client
retries. Assertions check acknowledged values, minority read rejection, convergence,
and sequential restart recovery. Additional JUnit cases cover durable-file reopen,
checksum rejection, forced votes, and virtual election timing. Reports are emitted
by Maven Surefire under `target/surefire-reports/`. The simulator exercises the same
core through an in-memory transport; timing entries distinguish virtual milliseconds
from real gRPC/disk latency.

For live gRPC fault exercises, each node optionally reads a fault file containing
`GROUP PEER` lines, where `-1` selects every group. A matching rule drops exchanges
to and from that peer. Write a replacement file and rename it atomically:

```sh
printf '%s\n' '-1 1' '-1 2' '-1 3' '-1 4' > cluster-data/node-0/faults.next
mv cluster-data/node-0/faults.next cluster-data/node-0/faults.txt
# Heal the partition:
rm cluster-data/node-0/faults.txt
```

A malformed/unreadable fault file drops peer traffic until corrected. Killing a
process and relaunching the same node ID against its existing directory exercises
durable recovery. The protobuf `Status` RPC exposes each group's role, term, leader,
commit index, and health for inspection.

Core design follows the [Raft paper](https://raft.github.io/raft.pdf); transport
uses [gRPC Java](https://grpc.io/docs/languages/java/). MIT licensed.
