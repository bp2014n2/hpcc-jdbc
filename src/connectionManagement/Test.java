package connectionManagement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Test {
	public static void main(String[] args) throws ClassNotFoundException {
		try {
			Class.forName("connectionManagement.HPCCDriver");
			Properties connprops = new Properties();
			connprops.put("PageOffset", "asdas");
			connprops.put("EclResultLimit", "20as00");
			HPCCConnection connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc").connect("http://192.168.56.101", connprops);
			//HPCCConnection connection = (HPCCConnection) DriverManager.getConnection("http://192.168.56.101", "test", "test");
			
			/* create HPCCStatement object for single use SQL query execution */
			 HPCCStatement stmt = (HPCCStatement) connection.createStatement();
			 
			/* Create your SQL query */
			 String mysql = "select * from i2b2demodata::test where patient_num < 10";
			/* Execute your SQL query */
			 HPCCResultSet res1 = (HPCCResultSet) stmt.executeQuery(mysql);
			 
			 while(res1.next()){
				 System.out.println(res1.getString(1));
			 }
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
