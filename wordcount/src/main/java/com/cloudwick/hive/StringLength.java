package com.cloudwick.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class StringLength extends UDF {

	private Text result = new Text();

	public Text evaluate(Text str) {
		if (str == null) {
			return null;
		}
    String s = str.toString();
    int i = str.getLength();
    String x = Integer.toString(i);
    return new Text(x);
    
	}
}
