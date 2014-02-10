package com.cloudwick.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SSMapper1 extends Mapper<LongWritable, Text, IntPair, Text>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		int i = 0;
		String[] words = value.toString().split(",");
		String s = words[0]+","+words[1]; 
		context.write(new IntPair(Integer.parseInt(words[2]),i),new Text(s) );
		
		
		
	}
	
	
	

}
