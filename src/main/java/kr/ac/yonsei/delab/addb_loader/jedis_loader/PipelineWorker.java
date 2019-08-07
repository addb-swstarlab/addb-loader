package kr.ac.yonsei.delab.addb_loader.jedis_loader;

import redis.clients.addb_jedis.Jedis;
import redis.clients.addb_jedis.Pipeline;

public class PipelineWorker implements Runnable {
	Jedis jedis_;
	Pipeline pip_;
	
	public PipelineWorker(Jedis jedis, Pipeline pip) {
		this.jedis_ = jedis;
		this.pip_ = pip;
	}

	public void run() {
		pip_.sync();
		pip_.close();
		jedis_.close();		
	}

}
