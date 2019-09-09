package kr.ac.yonsei.delab.addb_loader.lettuce_loader;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.addb.FpWriteArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.netty.channel.EventLoopGroup;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PipelineManager {
    final private static int PIPELINE_COUNT = 1000;

    public static class Worker implements Runnable {
        List<RedisFuture<String>> futures;

        Worker(List<RedisFuture<String>> futures) {
            this.futures = futures;
        }

        public void run() {

        }
    }

    // Thread pool
    ExecutorService executorService;

    @Getter
    public StatefulRedisConnection<String, String> nodeConn;
    @Getter
    public RedisAsyncCommands<String, String> commands;
    private List<RedisFuture<String>> futures;

    public static PipelineManager create(StatefulRedisClusterConnection<String, String> conn,
                                         int slot,
                                         ExecutorService executorService) {
        RedisClusterNode node = conn.getPartitions().getPartitionBySlot(slot);
        return new PipelineManager(conn.getConnection(node.getNodeId()), executorService);
    }

    public PipelineManager(StatefulRedisConnection<String, String> nodeConn, ExecutorService executorService) {
        this.nodeConn = nodeConn;
        this.commands = nodeConn.async();
        this.commands.setAutoFlushCommands(false);
        this.futures = new ArrayList<>();
        this.executorService = executorService;
    }

    public void fpwrite(FpWriteArgs args) {
        if (futures.size() > PIPELINE_COUNT) {
            commands.flushCommands();

            this.executorService.execute(new Worker(futures));
            futures = new ArrayList<>();
        }

        futures.add(commands.fpwrite(args));
    }

    public void close() {
        nodeConn.close();
    }
}
