package kr.ac.yonsei.delab.addb_loader;

import java.lang.management.GarbageCollectorMXBean;
import java.util.Arrays;
import java.util.List;

import redis.clients.addb_jedis.Protocol;
import redis.clients.addb_jedis.util.CommandArgsObject;
import redis.clients.addb_jedis.exceptions.JedisClusterException;


public class RedisClient {
	String table_id_ = null;
	String data[];
	String partition_list[];
	int colCnt_ = 0;
	String partitionInfo_ = null;
	JedisManager jmanager_;
	
	public RedisClient(JedisManager jmanager, String table_id, String [] array, String [] partition_columns, int colCnt){
		table_id_ = table_id;
		data = new String[colCnt];
		partition_list = new String[partition_columns.length];
		System.arraycopy(array, 0, data, 0, colCnt);
		System.arraycopy(partition_columns, 0, partition_list, 0, partition_columns.length);
		this.colCnt_ = colCnt;
		jmanager_ = jmanager;
	}
	
	public void execute() {
		CommandArgsObject args = createArgsObject();
		RedisNode rn = jmanager_.retRedisNode(args.getDataKey());
//		System.out.println("selected :" + rn.Host_ + " and " + rn.Port_
//				+ " start slot " + rn.startSlot_ + " end slot " + rn.endSlot_);
		PipelineWorker worker = rn.execute(args);
		if(worker != null) {
			jmanager_.executorService.execute(worker);
		}
	}
	
	public CommandArgsObject createArgsObject() {
		String datakey = createDataKey();
		String columnCount = Integer.toString(colCnt_);
		String partitionInfo = partitionInfo_;
		List<String> dataList = Arrays.asList(data);
//		System.out.println("datakey = " + datakey);
//		System.out.println("columnCount = " + columnCount);
//		System.out.println("partitionInfo = " + partitionInfo);
//		for(int i = 0; i < dataList.size(); i++)
//			System.out.println("datalist = " + dataList.get(i));
		
		return new CommandArgsObject(datakey, columnCount, partitionInfo, dataList);
	}
	
	public String createPartitionInfo() {
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < partition_list.length; i++) {
		  builder.append(partition_list[i]);
		  builder.append(":");
		  builder.append(data[Integer.parseInt(partition_list[i]) - 1 ]);
		  if (i != partition_list.length -1) builder.append(":");
		}
		
		return builder.toString();
	}
	
	public String createDataKey() {
		StringBuilder builder = new StringBuilder();
		builder.append("D:{");
		builder.append(table_id_);
		builder.append(":");
		partitionInfo_ = createPartitionInfo();
		builder.append(partitionInfo_);
		builder.append("}");
		return builder.toString();
	}

}
