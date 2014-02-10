package com.cloudwick.pig;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class CustomFilter extends FilterFunc{

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Boolean exec(Tuple t) throws IOException {
		// TODO Auto-generated method stub
		
		int i = (Integer)t.get(0);
		if(i>500){
			return true;
		}
		else return false;
	}
	
	
	
	

}
