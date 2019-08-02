package kr.ac.yonsei.delab.addb_loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import kr.ac.yonsei.delab.addb_loader.JedisManager;

/**
 * Hello world!
 *
 */
public class Loader
{
    public static void main( String[] args )
    {
//       System.out.println( "ADDB Loader" );
    	/* Command type : ./Loader.jar -i lineitem.tbl -p 1,2,3 */
    	String table_name = null;
    	String partition_column = null;
    	String table_id = null;
    	if(args.length != 6) {
    		System.out.println( "ex) Loader.jar -i table_id -p table_name -c partition_column" );
    		return;
    	}
    	
    	for(int i=0; i < args.length; i++) {
    		if(args[i].equals("-i") || args[i].equals("-I")) {
    			table_id = args[i+1];
    		}
    		if(args[i].equals("-p") || args[i].equals("-P")) {
    			table_name = args[i+1];
    		}
    		
    		if(args[i].equals("-c") || args[i].equals("-C")) {
    			partition_column = args[i+1];
    		}
    	}
    	
    	if(table_id == null || table_name == null || partition_column == null) {
    		System.out.println( "invalid table name and partition column" );
    		return;
    	}
    	
    	String partition_columns[] = partition_column.split(",");
//    	for(int i=0; i< partition_columns.length; i++) {
//        	System.out.println("partition column " + partition_columns[i]);    		
//    	}
       long setup;
    	setup = System.currentTimeMillis();
    	JedisManager jManager = new JedisManager();
    	System.out.println("Setup time = " + ((System.currentTimeMillis() - setup) / 1000));
    	
    	File file = new File(table_name);
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
