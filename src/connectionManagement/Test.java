package connectionManagement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Test {
	public static void main(String[] args) {
		try {
			Class.forName("connectionManagement.HPCCDriver");
			
			Properties connectionProperties = new Properties();
			connectionProperties.put("EclResultLimit", "2000");
			HPCCConnection connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc").connect("http://192.168.56.101", connectionProperties);
			
			/* create HPCCStatement object for single use SQL query execution */
			 HPCCStatement stmt = (HPCCStatement) connection.createStatement();
			 
			/* Create your SQL query */
			 String query = "insert into i2b2demodata.modifier_dimension values('test','test','test','test','test','test','test','test',5);";
			 
			/* Execute your SQL query */
			 //HPCCResultSet res1 = (HPCCResultSet) 
					 stmt.executeQuery(query);
			 
			 /* Check the result set */
//			 while(res1.next()){
//				 System.out.println(res1.getString(3));
//			 }
		
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
