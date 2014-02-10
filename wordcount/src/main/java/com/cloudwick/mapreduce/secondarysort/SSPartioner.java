package com.cloudwick.mapreduce.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SSPartioner extends Partitioner<IntPair, Text>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(IntPair key, Text value, int numPartitions) {
		// TODO Auto-generated method stub
		return Math.abs(key.getFirst().get()*127)%numPartitions;
		
		
		
	}
	

}
