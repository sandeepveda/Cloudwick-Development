package com.cloudwick.mapreduce.empstats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.cloudwick.mapreduce.secondarysort.IntPair;

public class EMPReducer extends Reducer<IntPair, IntWritable, IntWritable, Text>{
	
	protected void reduce(IntPair key, Iterable<IntWritable> valList,
			Context context) throws IOException, InterruptedException{
		
		//Get Key
		
		IntWritable key1 = key.getFirst();
		
		ArrayList<IntWritable> a = new ArrayList<IntWritable>();
		for (IntWritable i : valList){
			a.add(i);
		}
		//Calculating Avg
		int length = a.size();
		int sum = 0;
		for (IntWritable i : a)
		{
			sum = sum+i.get();
		}
		 int avg = sum/length;
		
		
		
		//Displaying Max value
		
		int min = a.get(0).get();
		
		//Displaying Min value		
		
		int max = a.get(a.size()-1).get();
		
		String s = Integer.toString(avg); //+","+Integer.toString(max);//+","+Integer.toString(min);
		Iterator<IntWritable> it= valList.iterator();
		context.write(key1,new Text(it.next().toString()));
		
	}

}
