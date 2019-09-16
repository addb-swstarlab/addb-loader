package kr.ac.yonsei.delab.addb_loader.lettuce_loader;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.addb.FpWriteArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import kr.ac.yonsei.delab.addb_loader.Global;
import kr.ac.yonsei.delab.addb_loader.Loader;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class LettuceLoader implements Loader {
    private String tableId;
    private String fileName;
    private String[] partitionColumns;

    public static Loader create(String tableId, String fileName, String[] partitionColumns) {
        return new LettuceLoader(tableId, fileName, partitionColumns);
    }

    public LettuceLoader(String tableId, String fileName, String[] partitionColumns) {
        this.tableId = tableId;
        this.fileName = fileName;
        this.partitionColumns = partitionColumns;
    }

    public void start() {
        double total_time = 0;
        double total_time_start, total_time_end;
        double lettuce_time = 0;

        RedisURI uri = RedisURI.Builder.redis(Global.host, Global.port).build();
        ClientResources resources = DefaultClientResources.builder()
//                .ioThreadPoolSize(4)
//                .computationThreadPoolSize(4)
                .build();
        RedisClusterClient clusterClient = RedisClusterClient.create(resources, uri);
        StatefulRedisClusterConnection<String, String> conn = clusterClient.connect();

        total_time_start = System.currentTimeMillis();
        Path filePath = Paths.get(fileName);

        // Version 1 - Reactor API
//        reactorLoader(filePath, conn);

        // Version 2 - Future API
//        futureLoader(filePath, conn);

        // Version 3 - Pipeline Loader (Iteration base)
//        pipelineIterLoader(filePath, conn);

        // Version 4 - Pipeline Loader (Reactor base)
        pipelineReactorLoader(filePath, conn);

        conn.close();
        clusterClient.shutdown();

        total_time_end = System.currentTimeMillis();
        total_time = total_time_end - total_time_start;
        System.out.println("Total Time =    " + (total_time / 1000));
    }

    private void reactorLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        ReactiveFileReader reader = new ReactiveFileReader(filePath);
        reader.parse()
                .map(line -> line.split("\\|"))
                .flatMap(colValues -> {
                    FpWriteArgs args = FpWriteArgs.Builder
                            .dataKey(createDataKey(colValues))
                            .partitionInfo(createPartitionInfo(colValues))
                            .columnCount(String.valueOf(colValues.length))
                            .data(colValues);
                    return conn.reactive().fpwrite(args)
                            .subscribeOn(Schedulers.parallel());
                })
                .blockLast();
    }

    private void futureLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        ReactiveFileReader reader = new ReactiveFileReader(filePath);
        reader.parse()
                .map(line -> line.split("\\|"))
                .map(colValues -> {
                    FpWriteArgs args = FpWriteArgs.Builder
                            .dataKey(createDataKey(colValues))
                            .partitionInfo(createPartitionInfo(colValues))
                            .columnCount(String.valueOf(colValues.length))
                            .data(colValues);
                    return conn.async().fpwrite(args);
                })
                .publishOn(Schedulers.elastic())
                .map(future -> {
                    try {
                        return future.await(1, TimeUnit.HOURS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .blockLast();
    }

    private void pipelineIterLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String line;
        List<RedisFuture<String>> futures = new LinkedList<>();
        final int PIPELINE_COUNT = 100000;
        while ((line = getLineFromFile(reader)) != null) {
            String[] colValues = line.split("\\|");
            FpWriteArgs args = FpWriteArgs.Builder
                    .dataKey(createDataKey(colValues))
                    .partitionInfo(createPartitionInfo(colValues))
                    .columnCount(String.valueOf(colValues.length))
                    .data(colValues);
            futures.add(conn.async().fpwrite(args));
            if (futures.size() >= PIPELINE_COUNT) {
                try {
                    LettuceFutures.awaitAll(
                            1,
                            TimeUnit.HOURS,
                            futures.toArray(new RedisFuture[0])
                    );
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    futures.clear();
                }
            }
        }

        try {
            LettuceFutures.awaitAll(
                    1,
                    TimeUnit.HOURS,
                    futures.toArray(new RedisFuture[0])
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void pipelineReactorLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        final int WINDOW_SIZE = 100000;
        ReactiveFileReader reader = new ReactiveFileReader(filePath);
        reader.parse()
                .map(line -> line.split("\\|"))
                .map(colValues -> {
                    FpWriteArgs args = FpWriteArgs.Builder
                            .dataKey(createDataKey(colValues))
                            .partitionInfo(createPartitionInfo(colValues))
                            .columnCount(String.valueOf(colValues.length))
                            .data(colValues);
                    return conn.async().fpwrite(args);
                })
                .window(WINDOW_SIZE)
                .flatMap(futureFlux -> futureFlux
                        .collectList()
                        .flatMap(futures -> Mono.fromCallable(() -> LettuceFutures.awaitAll(
                                1,
                                TimeUnit.HOURS,
                                futures.toArray(new RedisFuture[0])
                        )))
                )
                .blockLast();
    }

    private <T> CompletableFuture<T> toCompletableFuture(Future<T> future) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private <T> CompletableFuture<T> toCompletableFuture(RedisFuture<T> redisFuture) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return redisFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String getLineFromFile(BufferedReader reader) {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String createPartitionInfo(String[] colValues) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < partitionColumns.length; ++i) {
            int partitionCol = Integer.parseInt(partitionColumns[i]);
            builder.append(partitionCol);
            builder.append(":");
            builder.append(colValues[partitionCol - 1]);
            if (i != partitionColumns.length - 1) {
                builder.append(":");
            }
        }
        return builder.toString();
    }

    private String createDataKey(String[] colValues) {
        StringBuilder builder = new StringBuilder();
        builder.append("D:{")
                .append(tableId)
                .append(":")
                .append(createPartitionInfo(colValues))
                .append("}");
        return builder.toString();
    }
}
