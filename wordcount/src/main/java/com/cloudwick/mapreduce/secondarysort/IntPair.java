package com.cloudwick.mapreduce.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class IntPair implements WritableComparable<IntPair> {

	private IntWritable first;
	private IntWritable second;

	public IntPair(int first, int second) {
		set(new IntWritable(first), new IntWritable(second));
	}

	public IntPair() {
		set(new IntWritable(),new IntWritable());
	}

	/**
	 * @return the first
	 */
	public IntWritable getFirst() {
		return first;
	}

	/**
	 * @return the second
	 */
	public IntWritable getSecond() {
		return second;
	}

	public void set(IntWritable first, IntWritable second) {
		this.first = first;
		this.second = second;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(arg0);
		second.readFields(arg0);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput arg0) throws IOException {

		first.write(arg0);
		second.write(arg0);
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return first.hashCode() * 163 + second.hashCode();
	}

	public boolean equals(Object o) {
		if (o instanceof IntPair) {
			IntPair ip = (IntPair) o;
			return first.equals(ip.first) && second.equals(ip.second);

		}
		return false;
	}

	public String toString() {
		return first.toString() + "##" + second.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(IntPair o) {
		// TODO Auto-generated method stub
		int cmp = first.compareTo(o.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(o.second);
	}

}
