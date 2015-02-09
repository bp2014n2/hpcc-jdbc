package connectionManagement;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Test {
	public static void main(String[] args) {
		try {
			Class.forName("connectionManagement.HPCCDriver");
			Driver jdbcdriver = DriverManager.getDriver("jdbc:hpcc");
			HPCCConnection connection = null;
			Properties connprops = new Properties();
			connprops.put("ServerAddress", "192.168.56.101");
			connprops.put("EclResultLimit", "100");
			String jdbcurl = "jdbc:hpcc;ServerAddress=192.168.124.128";
			connection = (HPCCConnection) jdbcdriver.connect(jdbcurl, connprops);
		
		
			/* create HPCCStatement object for single use SQL query execution */
			 HPCCStatement stmt = (HPCCStatement) connection.createStatement();
			 
			/* Create your SQL query */
			 String mysql = "select * from i2b2demodata::test where patient_num < 10";
			/* Execute your SQL query */
			 HPCCResultSet res1 = (HPCCResultSet) stmt.executeQuery(mysql);
			 
			 while(res1.next()){
				 System.out.println(res1.getString(1));
			 }
		
		} catch (SQLException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
