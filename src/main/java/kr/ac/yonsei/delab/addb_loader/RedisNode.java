package kr.ac.yonsei.delab.addb_loader;

import redis.clients.addb_jedis.Jedis;
import redis.clients.addb_jedis.JedisPool;
import redis.clients.addb_jedis.JedisPoolConfig;
import redis.clients.addb_jedis.Pipeline;
import redis.clients.addb_jedis.util.CommandArgsObject;

public class RedisNode implements Comparable {
	String Host_;
	int Port_;
	int startSlot_;
	int endSlot_;
	JedisPool pool;
	Jedis jedis;
	Pipeline pip;
	int refCount;
	
	public RedisNode(String Host, int Port, int startSlot, int endSlot) {
		Host_ = Host;
		Port_ = Port;
		startSlot_ = startSlot;
		endSlot_ = endSlot;
		refCount = 0;
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		pool = new JedisPool(jedisPoolConfig, Host_, Port_);
	}
	
	public int compareTo(Object obj) {
		RedisNode other = (RedisNode)obj;
		if(startSlot_ < other.startSlot_) return -1;
		else if (startSlot_ > other.startSlot_) return 1;
		else return 0;
	}
	
	public void execute(CommandArgsObject args) {
		if(jedis == null) {
			jedis = pool.getResource();
			pip = jedis.pipelined();
		}
	
		pip.fpwrite(args);
		//jedis.fpwrite(args);
		refCount++;
		if(refCount == 5000 ) {
			//pip.sync();
			//refCount = 0;
//			if(jedis != null) {
//				jedis.close();
//				jedis = null;
//			}	
		}
	}
	
	public void close() {
		if(jedis != null) {
			jedis.close();
		}	
		pool.close();
	}
}
