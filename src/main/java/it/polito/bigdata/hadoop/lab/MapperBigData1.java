package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    	
    		String[] lineParts = value.toString().split(",");
    		if(lineParts.length > 2){    			
	    		for(int i = 1; i < lineParts.length - 1; i++) {
	    			int j = i + 1;
	    			while(j < lineParts.length){
	    				if(lineParts[i].compareTo(lineParts[j]) > 0)context.write(new Text(lineParts[i] +","+ lineParts[j]), new IntWritable(1));						
	    				else context.write(new Text(lineParts[j] +","+ lineParts[i]), new IntWritable(1));					
	    				j++;
	    			}
	    			
	    		}
			}
    	
    }
}
