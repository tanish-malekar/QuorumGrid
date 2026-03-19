package dev.tanish.quorum;

import dev.tanish.quorum.wire.*;
import io.grpc.*;
import java.util.concurrent.TimeUnit;

/** Retry one logical command with the same client ID and sequence on unknown outcomes. */
public final class Client {
  public static void main(String[] args) throws Exception {
    if (args.length < 5)
      throw new IllegalArgumentException(
          "Client BASE_PORT CLIENT_ID SEQUENCE PUT|GET|ERASE KEY [VALUE]");
    int port = Integer.parseInt(args[0]);
    String key = args[4];
    int shard = Raft.shard(key);
    var command =
        new Raft.Command(
            Raft.Op.valueOf(args[3]),
            args[1],
            Long.parseLong(args[2]),
            key,
            args.length > 5 ? args[5] : "");
    Raft.validate(command);
    var request =
        ClientRequest.newBuilder().setGroup(shard).setCommand(Node.encode(command)).build();
    var channels = new ManagedChannel[5];
    try {
      for (int i = 0; i < 5; i++)
        channels[i] =
            ManagedChannelBuilder.forAddress("127.0.0.1", port + i).usePlaintext().build();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      int node = 0;
      while (System.nanoTime() < deadline) {
        try {
          var response =
              ClusterGrpc.newBlockingStub(channels[node])
                  .withDeadlineAfter(2200, TimeUnit.MILLISECONDS)
                  .execute(request);
          if (response.getCode() == ClientReply.Code.OK) {
            System.out.println(
                "group="
                    + shard
                    + " found="
                    + response.getFound()
                    + " value="
                    + response.getValue());
            return;
          }
          if (response.getCode() == ClientReply.Code.REJECTED)
            throw new IllegalStateException(response.getError());
          int hint = response.getLeader();
          node = hint >= 0 && hint < 5 && hint != node ? hint : (node + 1) % 5;
        } catch (StatusRuntimeException ex) {
          if (ex.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) throw ex;
          node = (node + 1) % 5;
        }
        Thread.sleep(20);
      }
      throw new IllegalStateException(
          "Unknown outcome: retry with the SAME client ID and sequence");
    } finally {
      for (var channel : channels) if (channel != null) channel.shutdownNow();
    }
  }
}
