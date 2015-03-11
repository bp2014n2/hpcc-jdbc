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
//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'anotherNewValue' where exists(select 1 from i2b2demodata.observation_fact where concept_cd='ATC:J01CE02' and i2b2demodata.query_global_temp.patient_num = i2b2demodata.observation_fact.patient_num)";
//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'anotherNewValue' where patient_num = 11";
			 String query = "update QUERY_GLOBAL_TEMP  set panel_count = 1"+ 
							 " where QUERY_GLOBAL_TEMP.panel_count = 0"+
							 " and exists"+
							 "   (select 1 from"+
							 "     (select f.patient_num  from i2b2demodata.observation_fact f"+  
							 "       where f.concept_cd IN"+
							 "         (select concept_cd  from i2b2demodata.concept_dimension"+
							 "           where concept_path LIKE '\\ICD\\M00-M99\\M50-M54\\M54\\%')"+
							 "       AND (f.MODIFIER_CD IN"+
							 "         (select MODIFIER_CD from i2b2demodata.MODIFIER_DIMENSION"+
							 "           where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%'))"+
							 "       AND (valtype_cd = 'T' AND tval_char IN ('G'))"+
							 "       AND (f.start_date >= '2011-01-01T00:00:00' "+
							 "         AND f.start_date <= '2012-01-01T00:00:00')"+
							 "       group by f.patient_num) t"+
							 "     where QUERY_GLOBAL_TEMP.patient_num = t.patient_num);";
//			 String query = "select concept_cd from i2b2demodata.concept_dimension where substring(concept_cd from '(ATC|ICD):.*' <> ''";

			 //String query = "insert into i2b2demodata.query_global_temp(patient_num,panel_count)values(123456,0);";
//			 String query = "insert into i2b2demodata.query_global_temp select modifier_cd as start_date, modifier_path as concept_cd from i2b2demodata.modifier_dimension;";


//			 String query = "insert into i2b2demodata.query_global_temp(concept_cd, start_date) select modifier_cd as concept_cd, download_date as start_date from i2b2demodata.modifier_dimension;";

			 
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
