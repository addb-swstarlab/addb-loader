package kr.ac.yonsei.delab.addb_loader.jedis_loader;

import kr.ac.yonsei.delab.addb_loader.Loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class JedisLoader implements Loader
{
	private String table_id;
	private String file_name;
	private String[] partition_columns;

	public static Loader create(String table_id, String file_name, String[] partition_columns) {
		return new JedisLoader(table_id, file_name, partition_columns);
	}

	public JedisLoader(String table_id, String file_name, String[] partition_columns) {
		this.table_id = table_id;
		this.file_name = file_name;
		this.partition_columns = partition_columns;
	}

    public void start() {
//       System.out.println( "ADDB Loader" );
       	long setup;
    	setup = System.currentTimeMillis();
    	JedisManager jManager = new JedisManager();
    	System.out.println("Setup time = " + ((System.currentTimeMillis() - setup) / 1000));
    	
    	File file = new File(file_name);
    	BufferedReader inFile = null;
    	double startTime, endTime;
    	double pt;
    	double duration = 0;
    	int colCnt = 0;
    	startTime = System.currentTimeMillis();
    	try {
        	inFile = new BufferedReader(new FileReader(file));
        	String line = null;
        	while ((line = inFile.readLine()) != null) {
        		String array[] = line.split("\\|");
        		if(colCnt == 0)  colCnt = array.length;
        		pt = System.currentTimeMillis();
        		RedisClient client = new RedisClient(jManager, table_id, array, partition_columns, colCnt);
        		client.execute();
        		duration += ((System.currentTimeMillis() - pt) /1000);
        	}
        	
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} finally {
    		try{
    			if ( inFile != null) {
    				inFile.close();
    			}
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    	endTime = System.currentTimeMillis();
    	System.out.println("duration = " + duration);
    	System.out.println("Read Time = " + ((endTime - startTime) / 1000));
    	long close = System.currentTimeMillis();
    	jManager.close();
    	System.out.println("Close Time = " + ((System.currentTimeMillis() - close) / 1000 ));
    }
}
