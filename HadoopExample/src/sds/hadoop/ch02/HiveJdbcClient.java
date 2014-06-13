package sds.hadoop.ch02;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.56.101:10000/default", "", "");
		Statement stmt = con.createStatement();
		
		// String tableName = "testHiveDriverTable";
		// ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
		
		ResultSet res = stmt.executeQuery("SELECT * FROM symbols");
		
		while (res.next()) {
			System.out.println(res.getString("stock_type") 
					+ res.getString("stock_symbol"));
		}
	}
}
