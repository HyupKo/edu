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
      String tableName = "testHiveDriverTable";

      ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
  }
} 
