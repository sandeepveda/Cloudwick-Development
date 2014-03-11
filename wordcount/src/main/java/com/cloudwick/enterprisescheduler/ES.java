package com.cloudwick.enterprisescheduler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ES {

	/**
	 * @param args
	 * @throws IOException
	 */

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(java.net.URI.create(uri), conf);

		try {
			boolean s1 = fs.exists(new Path(uri));
			System.out.println(s1);

		} finally {

		}

	}

}
