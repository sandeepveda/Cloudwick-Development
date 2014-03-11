package com.cloudwick.hive;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJDBC {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws SQLException {
		 try {
	          Class.forName(driverName);
	        } catch (ClassNotFoundException e) {
	          // TODO Auto-generated catch block
	          e.printStackTrace();
	          System.exit(1);
	        }
	      
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.1.139:10000/default", "", "");
		if (con.isClosed()) {
			System.out.println("connection is closed");			
		}
		else
		{
			System.out.println("connection is open");
			
			
		}
		
		 // show tables
	    //String sql = "show tables";
	    String sql = "select * from employee_managed";
	    System.out.println("Running: " + sql);
	    
	    Statement stmt = con.createStatement();
	    
	    ResultSet res = stmt.executeQuery(sql);
	    while (res.next()) {
	      System.out.println(res.getString(1));
	    }
	    con.close();
	    System.out.println("Connection closed");

	}
		

	}


