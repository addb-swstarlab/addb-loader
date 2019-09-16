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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TaoPipelineManager {
    final private static int PIPELINE_COUNT = 1000;

    ExecutorService service = null;

    @Getter
    public StatefulRedisConnection<String, String> nodeConn;
    @Getter
    public RedisAsyncCommands<String, String> commands;
    private List<FpWriteArgs> argsList;

    public static TaoPipelineManager create(StatefulRedisClusterConnection<String, String> conn,
                                            int slot) {
        RedisClusterNode node = conn.getPartitions().getPartitionBySlot(slot);
        return new TaoPipelineManager(conn.getConnection(node.getNodeId()));
    }

    public static TaoPipelineManager create(StatefulRedisClusterConnection<String, String> conn,
                                            int slot,
                                            ExecutorService service) {
        RedisClusterNode node = conn.getPartitions().getPartitionBySlot(slot);
        return new TaoPipelineManager(conn.getConnection(node.getNodeId()), service);
    }

    public TaoPipelineManager(StatefulRedisConnection<String, String> nodeConn) {
        this.nodeConn = nodeConn;
        this.commands = nodeConn.async();
        this.commands.setAutoFlushCommands(false);
        this.argsList = new ArrayList<>();
    }

    public TaoPipelineManager(StatefulRedisConnection<String, String> nodeConn,
                              ExecutorService service) {
        this.nodeConn = nodeConn;
        this.commands = nodeConn.async();
        this.commands.setAutoFlushCommands(false);
        this.argsList = new ArrayList<>();
        this.service = service;
    }

    public void fpwrite(FpWriteArgs args) {
        if (argsList.size() >= PIPELINE_COUNT) {
            if (service != null) {
                service.execute(() -> pipeline(new ArrayList<>(argsList)));
            } else {
                pipeline(argsList);
            }
            argsList.clear();
            argsList = new ArrayList<>();
        }

        argsList.add(args);
    }

    private void pipeline(List<FpWriteArgs> argsList) {
        List<RedisFuture<String>> futures = argsList.stream()
                .map(_args -> commands.fpwrite(_args))
                .collect(Collectors.toList());
        commands.flushCommands();
        LettuceFutures.awaitAll(
                1,
                TimeUnit.HOURS,
                futures.toArray(new RedisFuture[0])
        );
        futures.clear();
        argsList.clear();
    }

    public void close() {
        pipeline(argsList);
    }
}
