package connectionManagement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public class Test {
	public static void main(String[] args) {
		try {
			Class.forName("connectionManagement.HPCCDriver");
			
			Properties connectionProperties = new Properties();
			connectionProperties.put("EclResultLimit", "2000");
			connectionProperties.put("ConnectTimeoutMilli", "30000");
			connectionProperties.put("username", "eha");
			connectionProperties.put("password", "eha");
			HPCCConnection connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc").connect("http://54.93.130.121", connectionProperties);
			
			/* create HPCCStatement object for single use SQL query execution */
			 HPCCStatement stmt = (HPCCStatement) connection.createStatement();
			 
			 String query ="drop table query_global_temp";
			/* Create your SQL query */

//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'anotherNewValue' where exists(select 1 from i2b2demodata.observation_fact where concept_cd='ATC:J01CE02' and i2b2demodata.query_global_temp.patient_num = i2b2demodata.observation_fact.patient_num)";
//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'anotherNewValue' where patient_num = 11";

//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'myNewValue' where exists(select 1 from (select f.patient_num from i2b2demodata.observation_fact f where concept_cd like 'ICD:%') t where query_global_temp.patient_num = t.patient_num)";
//			 String query = "select concept_cd from i2b2demodata.concept_dimension where substring(concept_cd from '(ATC|ICD):.*' <> ''";
//			 String query = "insert into i2b2demodata.query_global_temp(patient_num,panel_count)values(123456,0);";
//			 String query = "insert into i2b2demodata.query_global_temp select modifier_cd as start_date, modifier_path as concept_cd from i2b2demodata.modifier_dimension;";
//			 String query = "insert into i2b2demodata.query_global_temp(concept_cd, start_date) select modifier_cd as concept_cd, download_date as start_date from i2b2demodata.modifier_dimension;";
//			 String query = "insert into QUERY_GLOBAL_TEMP (patient_num, panel_count) select f.patient_num, 0 as panel_count from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd  from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\F00-F99\\F30-F39\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD  from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2012-01-01T00:00:00' AND f.start_date <= '2013-01-01T00:00:00') group by  f.patient_num;";
//			 String query = "insert into mySchema.myTable values (valueA, valueB, valueC)";
			 String query1 = "DROP TABLE QUERY_GLOBAL_TEMP;";
			 String query2 = "DROP TABLE DX";
			 String query3 = "CREATE TEMP TABLE QUERY_GLOBAL_TEMP(ENCOUNTER_NUM int,PATIENT_NUM int,INSTANCE_NUM int,CONCEPT_CD varchar(50),START_DATE TIMESTAMP,PROVIDER_ID varchar(50),PANEL_COUNT int,fact_count int,fact_panels int);";
			 String query4 = "CREATE TEMP TABLE DX(ENCOUNTER_NUM int,PATIENT_NUM int,INSTANCE_NUM int,CONCEPT_CD varchar(50),START_DATE TIMESTAMP,PROVIDER_ID varchar(50),temporal_start_date TIMESTAMP,temporal_end_date TIMESTAMP);";
			 String query5 = "insert into QUERY_GLOBAL_TEMP (patient_num, panel_count) with t as (select f.patient_num  from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd  from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\F00-F99\\F30-F39\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD  from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2012-01-01T00:00:00' AND f.start_date <= '2013-01-01T00:00:00') group by  f.patient_num) select t.patient_num, 0 as panel_count from t;";
			 String query5_working = "insert into QUERY_GLOBAL_TEMP (patient_num, panel_count) select f.patient_num, 0 as panel_count from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd  from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\F00-F99\\F30-F39\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD  from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2012-01-01T00:00:00' AND f.start_date <= '2013-01-01T00:00:00') group by  f.patient_num;";
			 String query6 = "update QUERY_GLOBAL_TEMP  set panel_count = 1 where QUERY_GLOBAL_TEMP.panel_count = 0 and exists (select 1 from (select f.patient_num  from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd  from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\M00-M99\\M50-M54\\M54\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2011-01-01T00:00:00' AND f.start_date <= '2012-01-01T00:00:00') group by f.patient_num) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num);";
			 String query7 = "update QUERY_GLOBAL_TEMP set panel_count = 2 where QUERY_GLOBAL_TEMP.panel_count = 1 and exists (select 1 from (select f.patient_num  from i2b2demodata.observation_fact f where f.provider_id IN (select provider_id  from i2b2demodata.provider_dimension where provider_path LIKE '\\PROVIDER\\ARZT\\10\\%') AND (f.start_date >= '2011-01-01T00:00:00' AND f.start_date <= '2012-01-01T00:00:00') group by  f.patient_num) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num);";
			 String query8 = "update QUERY_GLOBAL_TEMP set panel_count = -1 where QUERY_GLOBAL_TEMP.panel_count = 2 and exists (select 1 from (select f.patient_num  from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\F00-F99\\F30-F39\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2011-01-01T00:00:00' AND f.start_date <= '2012-01-01T00:00:00') group by f.patient_num) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num);";
			 String query9 = "insert into DX (patient_num) select * from (select distinct patient_num from QUERY_GLOBAL_TEMP where panel_count = 2) q;";
			 String query10 = "select count(distinct patient_num) as patient_num_count from DX;";
//			 ECLBuilder eclBuilder = new ECLBuilder();
//			 System.out.println(eclBuilder.generateECL(query));
			 
			 String queryQT = "INSERT INTO i2b2demodata.QT_QUERY_MASTER (name, USER_ID, group_id, create_date) VALUES ('JDBCLevelTest', 'demo', 'Demo', '2015-03-12 14:40:00.222') RETURNING *";
			 
			/* Execute your SQL query */
			 //HPCCResultSet res1 = (HPCCResultSet) 
			 printResultSet(stmt.executeQuery(queryQT));
			 
			 /* Check the result set */
//			 while(res1.next()){
//				 System.out.println(res1.getString(3));
//			 }#OPTION('name', 'java insert');
		
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	private static void printResultSet(ResultSet res) throws SQLException {

		if (!res.next()) {
			return;
		} else {
			res.first();
		}
		ArrayList<String[]> table = new ArrayList<String[]>();
		
		int columnCount = res.getMetaData().getColumnCount();
		do{
			String[] row = new String[columnCount];
			for(int i = 0; i<columnCount; i++) {
				row[i] = res.getString(i+1).trim();
			}
			table.add(row);
		 } while(res.next());
//		System.out.println(res.getMetaData().getColumnCount());
		int[] columnWidths = new int[columnCount];
		for (final String[] row : table) {
			for (int j=0; j<row.length; j++) {
				columnWidths[j] = java.lang.Math.max(columnWidths[j], row[j].length());
			}
		}
		for(int i = 0; i<table.size(); i++) {
			String[] row = table.get(i); 
			String stringFormat = "|";
			for (int j : columnWidths) {
				stringFormat += " %-"+(j+1)+"s|";
			}
//		    String stringFormat = "%15s%15s%15s%15s%15s%15s%15s%15s";
			System.out.printf(stringFormat+"\n", row);
		}
	}
}
