package com.cloudwick.pig;

import java.io.IOException;

import org.apache.commons.lang.math.Range;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

public class CustomLoadFunc extends LoadFunc {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.LoadFunc#getInputFormat()
	 */
	@Override
	public InputFormat getInputFormat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.LoadFunc#getNext()
	 */
	@Override
	public Tuple getNext() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.
	 * RecordReader,
	 * org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit)
	 */
	@Override
	public void prepareToRead(RecordReader arg0, PigSplit arg1)
			throws IOException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.LoadFunc#setLocation(java.lang.String,
	 * org.apache.hadoop.mapreduce.Job)
	 */
	@Override
	public void setLocation(String arg0, Job arg1) throws IOException {
		
		// TODO Auto-generated method stub

	}

	public CustomLoadFunc() {
		super();
		// TODO Auto-generated constructor stub
	}
	 

}
