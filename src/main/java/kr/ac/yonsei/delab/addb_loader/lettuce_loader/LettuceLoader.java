package kr.ac.yonsei.delab.addb_loader.lettuce_loader;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.addb.FpWriteArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.AsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import io.lettuce.core.support.ConnectionPoolSupport;
import jdk.nashorn.internal.objects.annotations.Setter;
import kr.ac.yonsei.delab.addb_loader.Global;
import kr.ac.yonsei.delab.addb_loader.Loader;
import kr.ac.yonsei.delab.addb_loader.jedis_loader.RedisClient;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.javaync.io.AsyncFiles;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

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

        // Version 2 - Async Pipeline API
//        conn.setAutoFlushCommands(false);
//        asyncPipelineLoader(filePath, conn);

        // Version 3 - Async Pool API
//        AsyncPool<StatefulRedisClusterConnection<String, String>> pool =
//            AsyncConnectionPoolSupport.createBoundedObjectPool(
//                    () -> clusterClient.connectAsync(StringCodec.UTF8),
//                    BoundedPoolConfig.create()
//            );
//        asyncPoolLoader(filePath, pool);
//        pool.close();

        // Version 4 - Sync Pool API
//        GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool = ConnectionPoolSupport
//                .createGenericObjectPool(
//                        () -> clusterClient.connect(),
//                        new GenericObjectPoolConfig());
//        syncPoolLoader(filePath, pool);

        // Version 5 - Reactor Pipeline API
//        reactorPipelineLoader(filePath, conn);

        // Version 6 - Reactor Pipeline Sync Pool API
//        GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool = ConnectionPoolSupport
//                .createGenericObjectPool(
//                        clusterClient::connect,
//                        new GenericObjectPoolConfig());
//        reactorPipelineSyncPoolLoader(filePath, pool);

        // Version 7 - Reactor Simple File Reader API
//        reactorSimpleFileLoader(filePath, conn);

        // Version 8 - Free from thread-pool hell referenced by Tao
        taoLoader2(filePath, conn);
//        taoLoader(filePath, conn)
//        taoLoader3(filePath, conn);
//        taoAsyncFileChannelLoader(filePath, conn);

        // Version 9 - Pipeline version on taoLoader
//        taoPipelineLoader(filePath, conn);

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
//                .publishOn(Schedulers.parallel())
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
//                .flatMap(colValues -> {
//                    FpWriteArgs args = FpWriteArgs.Builder
//                            .dataKey(createDataKey(colValues))
//                            .partitionInfo(createPartitionInfo(colValues))
//                            .columnCount(String.valueOf(colValues.length))
//                            .data(colValues);
//                    return conn.reactive().fpwrite(args)
//                            .subscribeOn(Schedulers.parallel());
//                })
//                .subscribeOn(Schedulers.elastic())
                .blockLast();
    }

    private void reactorPipelineLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        ReactiveFileReader reader = new ReactiveFileReader(filePath);
        reader.parse()
                .publishOn(Schedulers.elastic())
                .map(line -> line.split("\\|"))
                .groupBy(colValues -> SlotHash.getSlot(createDataKey(colValues)))
                .flatMap(groupedFlux -> groupedFlux
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
//                        .collectList()
//                        .map(futures -> {
//                            boolean result = LettuceFutures.awaitAll(
//                                    1,
//                                    TimeUnit.HOURS,
//                                    futures.toArray(new RedisFuture[0])
//                            );
//                            futures.clear();
//                            return result;
//                        })
                )
                .blockLast();
    }

    private void reactorSimpleFileLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String line;
        while ((line = getLineFromFile(reader)) != null) {
            String[] colValues = line.split("\\|");

            FpWriteArgs args = FpWriteArgs.Builder
                    .dataKey(createDataKey(colValues))
                    .partitionInfo(createPartitionInfo(colValues))
                    .columnCount(String.valueOf(colValues.length))
                    .data(colValues);
            conn.reactive().fpwrite(args)
                    .block();
        }

        try {
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void reactorPipelineSyncPoolLoader(Path filePath,
                                               GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool) {
        ReactiveFileReader reader = new ReactiveFileReader(filePath);
        reader.parse()
                .map(line -> line.split("\\|"))
                .groupBy(colValues -> SlotHash.getSlot(createDataKey(colValues)))
                .flatMap(groupedFlux -> groupedFlux
                        .flatMap(colValues -> {
                            StatefulRedisClusterConnection<String, String> conn;
                            try {
                                conn = pool.borrowObject();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            FpWriteArgs args = FpWriteArgs.Builder
                                    .dataKey(createDataKey(colValues))
                                    .partitionInfo(createPartitionInfo(colValues))
                                    .columnCount(String.valueOf(colValues.length))
                                    .data(colValues);
                            Mono<String> res = conn.reactive().fpwrite(args);
                            conn.close();
                            return res;
                        })
                        .subscribeOn(Schedulers.elastic())
                )
                .blockLast();
    }

    private void syncPoolLoader(Path filePath,
                                GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool) {
        Map<Integer, StatefulRedisClusterConnection<String, String>> connMap = new HashMap<>();
        ExecutorService service = Executors.newFixedThreadPool(6);

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String line;
        while ((line = getLineFromFile(reader)) != null) {
            String[] colValues = line.split("\\|");
            String dataKey = createDataKey(colValues);

            int slot = SlotHash.getSlot(dataKey);
            StatefulRedisClusterConnection<String, String> conn;
            if (!connMap.containsKey(slot)) {
                System.out.println("Create slot: " + slot);
                try {
                    conn = pool.borrowObject();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                connMap.put(slot, conn);
            } else {
                conn = connMap.get(slot);
            }

            FpWriteArgs args = FpWriteArgs.Builder
                    .dataKey(createDataKey(colValues))
                    .partitionInfo(createPartitionInfo(colValues))
                    .columnCount(String.valueOf(colValues.length))
                    .data(colValues);
            service.execute(() -> conn.sync().fpwrite(args));
        }

        try {
            service.shutdown();
            reader.close();
            pool.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void asyncPoolLoader(Path filePath,
                                 AsyncPool<StatefulRedisClusterConnection<String, String>> pool) {
        ExecutorService service = Executors.newFixedThreadPool(2);
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<CompletableFuture<String>> futures = new ArrayList<>();
        final int PIVOT = 20;
        String line;
        while ((line = getLineFromFile(reader)) != null) {
            String[] colValues = line.split("\\|");

            FpWriteArgs args = FpWriteArgs.Builder
                    .dataKey(createDataKey(colValues))
                    .partitionInfo(createPartitionInfo(colValues))
                    .columnCount(String.valueOf(colValues.length))
                    .data(colValues);

            CompletableFuture<String> result = pool.acquire()
                    .thenCompose(connection -> connection.async()
                            .fpwrite(args)
                            .whenComplete((s, throwable) -> pool.release(connection))
                    );
            futures.add(result);
            if (futures.size() >= PIVOT) {
                List<CompletableFuture<String>> fixedFutures = new ArrayList<>(futures);
                futures.clear();
                service.execute(() -> {
                    CompletableFuture.allOf(fixedFutures.toArray(new CompletableFuture[0]))
                            .join();
                    fixedFutures.clear();
                });
            }
        }

        try {
            service.shutdown();
            reader.close();
            pool.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void asyncPipelineLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        Map<Integer, PipelineManager> pipelineManagerMap = new HashMap<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String line;
        while ((line = getLineFromFile(reader)) != null) {
            String[] colValues = line.split("\\|");
            String dataKey = createDataKey(colValues);

            int slot = SlotHash.getSlot(dataKey);
            PipelineManager pipelineManager;
            if (!pipelineManagerMap.containsKey(slot)) {
                System.out.println("Create slot: " + slot);
                pipelineManager = PipelineManager.create(conn, slot, executorService);
                pipelineManagerMap.put(slot, pipelineManager);
            } else {
                pipelineManager = pipelineManagerMap.get(slot);
            }
            FpWriteArgs args = FpWriteArgs.Builder
                    .dataKey(dataKey)
                    .partitionInfo(createPartitionInfo(colValues))
                    .columnCount(String.valueOf(colValues.length))
                    .data(colValues);
            pipelineManager.fpwrite(args);
        }

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<Integer, PipelineManager> entry : pipelineManagerMap.entrySet()) {
            entry.getValue().close();
        }
        executorService.shutdown();
    }

    private void taoLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
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
                }
                futures.clear();
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

    private void taoLoader2(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
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
//                .buffer(WINDOW_SIZE)
//                .map(futures -> LettuceFutures.awaitAll(
//                        1,
//                        TimeUnit.HOURS,
//                        futures.toArray(new RedisFuture[0])
//                ))
                .window(WINDOW_SIZE)
                .flatMap(futureFlux -> futureFlux
                        .collectList()
                        .flatMap(futures -> {
                            System.out.println(futures.size() + ": " + Thread.currentThread().getName());
                            return Mono.fromCallable(() -> LettuceFutures.awaitAll(
                                    1,
                                    TimeUnit.HOURS,
                                    futures.toArray(new RedisFuture[0])
                            ));
                        })
                        .doOnNext(result -> System.out.println(Thread.currentThread().getName() + " Finished"))
                )
                .blockLast();
    }

    private void taoLoader3(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        final int WINDOW_SIZE = 100000;
        ReactiveFileReader reader = new ReactiveFileReader(filePath);
        reader.parse()
                .map(line -> line.split("\\|"))
                .flatMap(colValues -> {
                    FpWriteArgs args = FpWriteArgs.Builder
                            .dataKey(createDataKey(colValues))
                            .partitionInfo(createPartitionInfo(colValues))
                            .columnCount(String.valueOf(colValues.length))
                            .data(colValues);
                    return conn.reactive().fpwrite(args);
                })
//                .window(WINDOW_SIZE)
//                .flatMap(futureFlux -> futureFlux
//                        .flatMap(colValues -> {
//                            FpWriteArgs args = FpWriteArgs.Builder
//                                    .dataKey(createDataKey(colValues))
//                                    .partitionInfo(createPartitionInfo(colValues))
//                                    .columnCount(String.valueOf(colValues.length))
//                                    .data(colValues);
//                            return conn.reactive().fpwrite(args);
//                        })
//                )
//                .subscribeOn(Schedulers.elastic())
                .blockLast();
    }

    private void taoAsyncFileChannelLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        final int WINDOW_SIZE = 20000;

        Flux.from(AsyncFiles.lines(filePath.toFile().getPath()))
                  .map(line -> line.split("\\|"))
//                .map(colValues -> {
//                    FpWriteArgs args = FpWriteArgs.Builder
//                            .dataKey(createDataKey(colValues))
//                            .partitionInfo(createPartitionInfo(colValues))
//                            .columnCount(String.valueOf(colValues.length))
//                            .data(colValues);
//                    return conn.async().fpwrite(args);
//                })
//                .window(WINDOW_SIZE)
//                .flatMap(futureFlux -> futureFlux
//                        .collectList()
//                        .map(futures -> LettuceFutures.awaitAll(
//                                1,
//                                TimeUnit.HOURS,
//                                futures.toArray(new RedisFuture[0])
//                        ))
//                )
                .blockLast();
    }

    private CompletableFuture<Integer> fileReadAsync(AsynchronousFileChannel fc, ByteBuffer buffer, int pos) {
        return toCompletableFuture(fc.read(buffer, pos));
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

    private void taoPipelineLoader(Path filePath, StatefulRedisClusterConnection<String, String> conn) {
        Map<Integer, TaoPipelineManager> pipelineManagerMap = new HashMap<>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(filePath.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String line;
        while ((line = getLineFromFile(reader)) != null) {
            String[] colValues = line.split("\\|");
            String dataKey = createDataKey(colValues);

            int slot = SlotHash.getSlot(dataKey);
            TaoPipelineManager pipelineManager;
            if (!pipelineManagerMap.containsKey(slot)) {
                System.out.println("Create slot: " + slot);
                pipelineManager = TaoPipelineManager.create(conn, slot);
                pipelineManagerMap.put(slot, pipelineManager);
            } else {
                pipelineManager = pipelineManagerMap.get(slot);
            }
            FpWriteArgs args = FpWriteArgs.Builder
                    .dataKey(dataKey)
                    .partitionInfo(createPartitionInfo(colValues))
                    .columnCount(String.valueOf(colValues.length))
                    .data(colValues);
            pipelineManager.fpwrite(args);
        }

        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<Integer, TaoPipelineManager> entry : pipelineManagerMap.entrySet()) {
            entry.getValue().close();
        }
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
