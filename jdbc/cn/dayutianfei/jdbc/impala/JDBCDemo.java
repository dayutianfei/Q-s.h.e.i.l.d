package cn.dayutianfei.jdbc.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCDemo {

	// using the dirver from impala
	
	 static String JDBCDriver = "com.cloudera.impala.jdbc41.Driver";
	// Define a string as the connection URL
	 private static final String CONNECTION_URL = "jdbc:impala://172.16.2.206:21050";
	 

	 /*
	// using the dirver from hive
	static String JDBCDriver = "org.apache.hive.jdbc.HiveDriver";
	// Define a string as the connection URL
	private static final String CONNECTION_URL = "jdbc:hive2://172.16.2.202:21050/;auth=noSasl";
    */
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Connection con = null;
		Statement stmt = null;
		PreparedStatement prep = null;
		ResultSet rs = null;
//		String query = "select * from test";
		String query = "show databases";
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
				System.out.println(rs.getString(1));
				count++;
				if (count > 10) {
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
	} // End main
}
