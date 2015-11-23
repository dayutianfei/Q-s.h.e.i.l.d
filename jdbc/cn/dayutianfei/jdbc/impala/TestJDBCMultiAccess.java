package cn.dayutianfei.jdbc.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class TestJDBCMultiAccess  implements Runnable{

	// using the dirver from impala
	/*
	 * static String JDBCDriver = "com.cloudera.impala.jdbc41.Driver"; // Define
	 * a string as the connection URL private static final String CONNECTION_URL
	 * = "jdbc:impala://172.16.8.34:21050";
	 */

	// using the dirver from hive
	static String JDBCDriver = "org.apache.hive.jdbc.HiveDriver";
	// Define a string as the connection URL
	private static final String CONNECTION_URL = "jdbc:hive2://172.16.8.34:21050/;auth=noSasl";

	public TestJDBCMultiAccess() {

	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int threadNumber = 24;
		TestJDBCMultiAccess instance = new TestJDBCMultiAccess();
		Thread[] threads = new Thread[threadNumber];
		for(int i = 0 ; i <threadNumber; i++){
			threads[i]=new Thread(instance,"thread-"+i);
		}
		for(int i = 0 ; i <threadNumber; i++){
			threads[i].start();
		}
	} // End main


	@Override
	public void run() {
		Connection con = null;
		Statement stmt = null;
		PreparedStatement prep = null;
		ResultSet rs = null;
		String query = "select * from test";
		Random r = new Random();
		int max = r.nextInt(20);
		try {
			Class.forName(JDBCDriver);
			con = DriverManager.getConnection(CONNECTION_URL);
			System.out.println("get connection success");
			stmt = con.createStatement();
			System.out.println("create statement success");
			rs = stmt.executeQuery(query);
			System.out.println("do the query success");
			int count = 0;
			while (rs.next()) {
				// 结果列从下标1开始
				System.out.println(rs.getInt(1));
				count++;
				if (count > max) {
					break;
				}
			}
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (stmt != null) {
					stmt.close();
				}
				if (prep != null) {
					prep.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException se2) {
				se2.printStackTrace();
			} // End try
		} // End try
		
	}
}
