package kr.ac.yonsei.delab.addb_loader;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.addb_jedis.Jedis;
import redis.clients.addb_jedis.JedisPool;
import redis.clients.addb_jedis.JedisPoolConfig;
import redis.clients.addb_jedis.Pipeline;
import redis.clients.addb_jedis.Protocol;
import redis.clients.addb_jedis.util.CommandArgsObject;

import kr.ac.yonsei.delab.addb_loader.RNWorker;

public class RedisNode implements Comparable {
	String Host_;
	int Port_;
	int startSlot_;
	int endSlot_;
	JedisPool pool;
	Jedis jedis;
	Pipeline pip;
	int Refcount = 0;

//	Queue<CommandArgsObject> dataQueue;
//	List<RNWorker> worklist;
	
	public RedisNode(String Host, int Port, int startSlot, int endSlot) {
		Host_ = Host;
		Port_ = Port;
		startSlot_ = startSlot;
		endSlot_ = endSlot;
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		pool = new JedisPool(jedisPoolConfig, Host_, Port_, 3000000, null, Protocol.DEFAULT_DATABASE);
		
		jedis = pool.getResource();
		pip = jedis.pipelined();
//		dataQueue = new ConcurrentLinkedQueue<CommandArgsObject>();
//		worklist = new ArrayList<RNWorker>(2);
//		for(int i = 0; i < 2; i++) {
//			RNWorker rn = new RNWorker(pool, 0, dataQueue);
//			worklist.add(rn);
//			rn.start();
//		}
	}
	
	public int compareTo(Object obj) {
		RedisNode other = (RedisNode)obj;
		if(startSlot_ < other.startSlot_) return -1;
		else if (startSlot_ > other.startSlot_) return 1;
		else return 0;
	}
	
	public PipelineWorker execute(CommandArgsObject args) {
		//System.out.println("queue insert = " + dataQueue.size());
		//dataQueue.offer(args);
//		Jedis jedis = pool.getResource();
//		Pipeline pip = jedis.pipelined();
		
		pip.fpwrite(args);
		Refcount++;
		if (Refcount == 200) {
			PipelineWorker worker = new PipelineWorker(this.jedis, this.pip);
			this.jedis = pool.getResource();
			this.pip = jedis.pipelined();
			Refcount = 0;
			return worker;
		}
		return null;
	}
	
	public void close() {
//		for(int i = 0; i < 2; i++) {
//			worklist.get(i).interrupt();
//		}
		
//		Jedis tmp_jedis = pool.getResource();
//		
//		while (dataQueue.size() != 0) {
//			CommandArgsObject arg = dataQueue.poll();
//			if (arg != null)	tmp_jedis.fpwrite(arg);
//		}
//		
//		tmp_jedis.close();
		if(this.pip != null) pip.close();
		if(this.jedis != null) jedis.close();
		
		pool.close();
	}
}
