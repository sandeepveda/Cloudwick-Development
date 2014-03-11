import static org.junit.Assert.*;

import org.apache.pig.pigunit.PigTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class PigTestMax {

	@Test
	public void test() {

		String[] args;
		String[] input = new String[2];
		input[1] = "7499,ALLEN,SALESMAN,7698,20-FEB-1981,1600,300,30";
		input[2] = "7521,WARD,SALESMAN,7698,22-FEB-1981,1250,500,30";
		
		String[] output = new String[1];
		output[1]= "(30,1600)";
		//PigTest t = new PigTest("C:\\Users\sandeep.veda\git\Hadoop\wordcount\maxsal.pig",input);
				
 
		
	}

	@BeforeClass
	static void beforeTest() {

	}

}
