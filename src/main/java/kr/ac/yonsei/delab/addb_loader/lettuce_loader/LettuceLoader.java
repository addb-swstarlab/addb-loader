package kr.ac.yonsei.delab.addb_loader.lettuce_loader;

import io.lettuce.core.RedisURI;
import io.lettuce.core.addb.FpWriteArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.AsyncConnectionPoolSupport;
import io.lettuce.core.support.AsyncPool;
import io.lettuce.core.support.BoundedPoolConfig;
import kr.ac.yonsei.delab.addb_loader.Global;
import kr.ac.yonsei.delab.addb_loader.Loader;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Path;
import java.nio.file.Paths;

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
//                .ioThreadPoolSize(8)
//                .computationThreadPoolSize(8)
                .build();
        RedisClusterClient clusterClient = RedisClusterClient.create(resources, uri);
        StatefulRedisClusterConnection<String, String> conn = clusterClient.connect();

//        AsyncPool<StatefulRedisClusterConnection<String, String>> pool =
//                AsyncConnectionPoolSupport.createBoundedObjectPool(
//                        () -> clusterClient.connectAsync(StringCodec.UTF8),
//                        BoundedPoolConfig.create()
//                );

        total_time_start = System.currentTimeMillis();
        Path filePath = Paths.get(fileName);
        ReactiveFileReader reader = new ReactiveFileReader(filePath);

        reader.parse()
                .map(line -> line.split("\\|"))
                .publishOn(Schedulers.elastic())
                .flatMap(colValues -> {
                    FpWriteArgs args = FpWriteArgs.Builder
                            .dataKey(createDataKey(colValues))
                            .partitionInfo(createPartitionInfo(colValues))
                            .columnCount(String.valueOf(colValues.length))
                            .data(colValues);
//                    return Mono.just(clusterClient.connect().sync().fpwrite(args));
                    return conn.reactive().fpwrite(args);
//                    return Mono.fromFuture(pool.acquire())
//                            .flatMap(connection -> connection.reactive().ping());
                })
                .subscribeOn(Schedulers.elastic())
                .blockLast();

//        pool.close();
        clusterClient.shutdown();
//        try {
//            String line = null;
//            int colCnt;
//            while ((line = fileReader.readLine()) != null) {
//                String[] colValues = line.split("\\|");
//                colCnt = colValues.length;
//                lettuce_time_start = System.currentTimeMillis();
//                String dataKey = createDataKey(colValues);
//                lettuce_time_end = System.currentTimeMillis();
//                lettuce_time += lettuce_time_end - lettuce_time_start;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }

//        try {
//            fileReader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
        total_time_end = System.currentTimeMillis();
        total_time = total_time_end - total_time_start;
        System.out.println("Total Time =    " + (total_time / 1000));
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
