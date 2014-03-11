package com.cloudwick.pig;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CustomEval extends EvalFunc<Integer> {

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Integer exec(Tuple balance) throws IOException {
		// TODO Auto-generated method stub
		int i = (Integer)balance.get(0);
		int newbal = i+1000;
		
		return newbal;
	}
	
	
	

}
