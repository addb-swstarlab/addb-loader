package kr.ac.yonsei.delab.addb_loader.lettuce_loader;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.addb.FpWriteArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TaoPipelineManager {
    final private static int PIPELINE_COUNT = 1000;

    @Getter
    public StatefulRedisConnection<String, String> nodeConn;
    @Getter
    public RedisAsyncCommands<String, String> commands;
    private List<RedisFuture<String>> futures;

    public static TaoPipelineManager create(StatefulRedisClusterConnection<String, String> conn,
                                            int slot) {
        RedisClusterNode node = conn.getPartitions().getPartitionBySlot(slot);
        return new TaoPipelineManager(conn.getConnection(node.getNodeId()));
    }

    public TaoPipelineManager(StatefulRedisConnection<String, String> nodeConn) {
        this.nodeConn = nodeConn;
        this.commands = nodeConn.async();
        this.commands.setAutoFlushCommands(false);
        this.futures = new ArrayList<>();
    }

    public void fpwrite(FpWriteArgs args) {
        if (futures.size() > PIPELINE_COUNT) {
            commands.flushCommands();
            sync();
            futures.clear();
        }

        futures.add(commands.fpwrite(args));
    }

    public void sync() {
        LettuceFutures.awaitAll(
                1,
                TimeUnit.HOURS,
                futures.toArray(new RedisFuture[0])
        );
    }

    public void close() {
        sync();
        nodeConn.close();
    }
}
