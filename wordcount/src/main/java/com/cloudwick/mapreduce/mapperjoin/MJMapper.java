package com.cloudwick.mapreduce.mapperjoin;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.cloudwick.mapreduce.secondarysort.IntPair;

public class MJMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
	 * Mapper.Context)
	 */

	HashMap<Integer, String> hashMap = new HashMap<Integer, String>();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		String fileName = "dept.txt";
		fileRead(fileName);

	}

	public void fileRead(String fileName) throws IOException {

		FileReader fread = new FileReader(fileName);
		BufferedReader bread = new BufferedReader(fread);

		try {
			String line;
			while ((line = bread.readLine()) != null) {
				String[] words = line.split(",");

				hashMap.put(Integer.parseInt(words[0]), words[1]);

			}
		}

		finally {
			fread.close();

		}

	}

	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		// int i = 0;
		String s = "";
		String[] words = value.toString().split(",");
		// String s = words[0] + "," + words[1];
		int i = Integer.parseInt(words[2]);
		if (i == 10) {
			s = words[0] + "," + words[1] + "," + hashMap.get(10);

		} else if (i == 20) {
			s = words[0] + "," + words[1] + "," + hashMap.get(20);

		}

		context.write(null, new Text(s));

	}

}
