package kr.ac.yonsei.delab.addb_loader.jedis_loader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kr.ac.yonsei.delab.addb_loader.Global;
import redis.clients.addb_jedis.*;
import redis.clients.addb_jedis.util.JedisClusterCRC16;
import redis.clients.addb_jedis.util.SafeEncoder;

public class JedisManager {
	List<RedisNode> jedisClusterNodes;
	ExecutorService executorService; 
	public JedisManager() {
		jedisClusterNodes = new ArrayList<RedisNode>();

		executorService = Executors.newFixedThreadPool(6);
		createJedisCluster();
		Collections.sort(jedisClusterNodes);
	}
	
	public RedisNode retRedisNode(String key) {
		int slot = JedisClusterCRC16.getSlot(key);

		int start = 0;
		int end = jedisClusterNodes.size() - 1;
		int pos = 0;

		while (true) {
			int middle = (start + end) / 2;
			int startSlot = jedisClusterNodes.get(middle).startSlot_;
			int endSlot = jedisClusterNodes.get(middle).endSlot_;
			if( startSlot <= slot && slot <= endSlot ) {
				pos = middle;
				break;
			} else if (  slot < startSlot ) {
				end = middle - 1;
			} else if ( endSlot < slot ) {
				start = middle + 1;
			}
		}
		 
//		System.out.println("target slot = " + slot);
//		System.out.println("node target = " +
//		jedisClusterNodes.get(pos).startSlot_ + " ~ "
//				+ jedisClusterNodes.get(pos).endSlot_);
		return jedisClusterNodes.get(pos);
	}
	
	public void createJedisCluster() {
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		/* Master Instance */
		JedisPool pool = new JedisPool(jedisPoolConfig, Global.host, Global.port);
		Jedis jedis = pool.getResource();
		List<Object> slotinfos = jedis.clusterSlots();
		for (int i = 0; i < slotinfos.size(); i++) {
			List<Object> slotinfo = (List<Object>) slotinfos.get(i);
	       int sPos = Integer.parseInt(slotinfo.get(0).toString());
	       int ePos = Integer.parseInt(slotinfo.get(1).toString());
			
	       for(int j = 0; j < slotinfo.size() - 2; j++) {
	    	   List<Object> clusterinfo = (List<Object>) slotinfo.get(j + 2);
	    	   String host = SafeEncoder.encode((byte[])clusterinfo.get(0));
	    	   int port = Integer.parseInt(clusterinfo.get(1).toString());
	    	   //System.out.println("host and Port" + host + " and " + port);
	    	   jedisClusterNodes.add(new RedisNode(host, port, sPos, ePos));
	       }
		}
		
		if(jedis != null) {
			jedis.close();
		}
		pool.close();
	}
	
	public void close() {
		
	  executorService.shutdown();
	  for (int i = 0; i < jedisClusterNodes.size(); i++) {
//		  if(jedisClusterNodes.get(i).pip != null) {
//			 jedisClusterNodes.get(i).pip.sync();
//		  }
		  
		  jedisClusterNodes.get(i).close();
	  }
	}

}


