package com.cloudwick.mapreduce.secondarysort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class SSDriver extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
            return -1;
		}
		
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(),"my job");
		job.setJarByClass(SSDriver.class);
		//job.setJobName(getClass().getName());
		
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//Code to set multiple inputs
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,SSMapper1.class );
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,SMapper2.class );
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//job.setMapperClass(SSMapper1.class);
		//job.setMapperClass(SMapper2.class);
		
		job.setPartitionerClass(SSPartioner.class);
		job.setSortComparatorClass(SSKeyComparator.class);
		job.setGroupingComparatorClass(SSGroupcomparator.class);
		job.setReducerClass(SSReducer.class);
		
		
		job.setMapOutputKeyClass(IntPair.class);
		//job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		//job.setOutputValueClass(Text.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		if(job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
		
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SSDriver(), args));
		// TODO Auto-generated method stub

	}

}
