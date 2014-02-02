package com.cloudwick.mapreduce.uniqueurl;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.classfile.Opcode.Set;

public class UUReducer extends Reducer<Text, Text, Text, IntWritable>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> valList,
			Context context)
			throws IOException, InterruptedException {
		HashSet<String> s = new HashSet<String>();
		for (Text t :valList)
		{
			s.add(t.toString());
		}
		int val = s.size();
		context.write(key, new IntWritable(val));
		
	}
	
	

}
