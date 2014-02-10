package com.cloudwick.mapreduce.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SSGroupcomparator extends WritableComparator {

	protected SSGroupcomparator() {
		super(IntPair.class, true);
	}
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// TODO Auto-generated method stub

		IntPair ip1 = (IntPair)w1;
		IntPair ip2 = (IntPair)w2;
		
		
		return ip1.getFirst().compareTo(ip2.getFirst());
		
	}

}
