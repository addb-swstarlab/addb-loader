package kr.ac.yonsei.delab.addb_loader;

import java.util.Queue;

import redis.clients.addb_jedis.Jedis;
import redis.clients.addb_jedis.JedisPool;
import redis.clients.addb_jedis.Pipeline;
import redis.clients.addb_jedis.util.CommandArgsObject;

public class RNWorker extends Thread {
	Jedis jedis_;
	Pipeline pip_;
	int refCount_;
	Queue<CommandArgsObject> queue_;
	
	public RNWorker(JedisPool pool, int refCount, Queue<CommandArgsObject> queue) {
		this.jedis_ = pool.getResource();
		this.pip_ = jedis_.pipelined();
		this.refCount_ = refCount;
		this.queue_ = queue;
	}
	
	@Override
	public void run() {
		while(!Thread.currentThread().isInterrupted()) {
			CommandArgsObject args = queue_.poll();
			if (args != null) {
			  pip_.fpwrite(args);
			  refCount_++;
			  if(refCount_ == 200) {
				  refCount_ = 0;
				  pip_.close();
				  pip_ = jedis_.pipelined();				  
			  }
			}
		}
		if(refCount_ != 0)	pip_.sync();
		jedis_.close();
		
	}
}
