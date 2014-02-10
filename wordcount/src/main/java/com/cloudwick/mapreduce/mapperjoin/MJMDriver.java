package com.cloudwick.mapreduce.mapperjoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class MJMDriver extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
            return -1;
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(MJMDriver.class);
		job.setJobName(getClass().getName());
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MJMapper.class);
		//job.setReducerClass(UUReducer.class);
		//job.setCombinerClass(WCReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(IntWritable.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		if(job.waitForCompletion(true)) {
			return 0;
		}
		return 1;
		
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MJMDriver(), args));
		// TODO Auto-generated method stub

	}
}
