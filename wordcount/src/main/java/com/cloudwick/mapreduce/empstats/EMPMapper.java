package com.cloudwick.mapreduce.empstats;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudwick.mapreduce.secondarysort.IntPair;

public class EMPMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>{
	
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException{
		
		String[] words = value.toString().split(",");
		IntPair mykey= new IntPair(Integer.parseInt(words[0].trim()),Integer.parseInt(words[1].trim()));
		
		context.write(mykey, new IntWritable(Integer.parseInt(words[1].trim())));
		
	}

}
