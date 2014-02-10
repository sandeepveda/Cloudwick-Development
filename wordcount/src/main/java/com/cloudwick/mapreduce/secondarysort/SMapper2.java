package com.cloudwick.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMapper2 extends Mapper<LongWritable, Text, IntPair, Text> {
	
	
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		int i = 1;
		String[] words = value.toString().split(",");
		//String s = words[1]+","+words[2]; 
		context.write(new IntPair(Integer.parseInt(words[0]),i),new Text(words[1]) );
		
		
		
	}

}
