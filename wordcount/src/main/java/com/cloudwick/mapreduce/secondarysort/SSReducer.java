package com.cloudwick.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SSReducer extends Reducer<IntPair, Text, IntWritable, Text>{
	
	@Override
	protected void reduce(IntPair key1, Iterable<Text> valList,
			Context context)
			throws IOException, InterruptedException {
		
		IntWritable key = key1.getFirst();
		for(Text t : valList){
			context.write(key, t);
		}
		
	}

}
