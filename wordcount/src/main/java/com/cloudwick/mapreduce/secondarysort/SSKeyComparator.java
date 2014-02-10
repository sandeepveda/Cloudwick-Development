package com.cloudwick.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SSKeyComparator extends WritableComparator {
	
	protected SSKeyComparator(){
		super(IntPair.class,true);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.
	 * WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// TODO Auto-generated method stub

		IntPair ip1 = (IntPair)w1;
		IntPair ip2 = (IntPair)w2;
		
		int cmp = ip1.getFirst().compareTo(ip2.getFirst());
		if(cmp!=0){
			return cmp;
		}
		return ip1.getSecond().compareTo(ip2.getSecond());
		
	}

}
