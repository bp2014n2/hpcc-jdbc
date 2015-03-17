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
			connectionProperties.put("username", "eha");
			connectionProperties.put("password", "eha");
			HPCCConnection connection = (HPCCConnection) DriverManager.getDriver("jdbc:hpcc://54.93.130.121:8010/i2b2").connect("jdbc:hpcc://54.93.130.121:8010/i2b2", connectionProperties);
			
			/* create HPCCStatement object for single use SQL query execution */
			 HPCCStatement stmt = (HPCCStatement) connection.createStatement();
			 
			/* Create your SQL query */

//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'anotherNewValue' where exists(select 1 from i2b2demodata.observation_fact where concept_cd='ATC:J01CE02' and i2b2demodata.query_global_temp.patient_num = i2b2demodata.observation_fact.patient_num)";
//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'anotherNewValue' where patient_num = 11";

//			 String query = "update i2b2demodata.query_global_temp set concept_cd = 'myNewValue' where exists(select 1 from (select f.patient_num from i2b2demodata.observation_fact f where concept_cd like 'ICD:%') t where query_global_temp.patient_num = t.patient_num)";
//			 String query = "select concept_cd from i2b2demodata.concept_dimension where substring(concept_cd from '(ATC|ICD):.*' <> ''";
//			 String query = "insert into i2b2demodata.query_global_temp(patient_num,panel_count)values(123456,0);";
//			 String query = "insert into i2b2demodata.query_global_temp select modifier_cd as start_date, modifier_path as concept_cd from i2b2demodata.modifier_dimension;";
//			 String query = "insert into i2b2demodata.query_global_temp(concept_cd, start_date) select modifier_cd as concept_cd, download_date as start_date from i2b2demodata.modifier_dimension;";
//			 String query = "insert into QUERY_GLOBAL_TEMP (patient_num, panel_count) select f.patient_num, 0 as panel_count from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd  from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\F00-F99\\F30-F39\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD  from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2012-01-01T00:00:00' AND f.start_date <= '2013-01-01T00:00:00') group by  f.patient_num;";
			 String query1 = "DROP TABLE QUERY_GLOBAL_TEMP;";
			 String query2 = "DROP TABLE DX";
			 String query3 = "CREATE TEMP TABLE QUERY_GLOBAL_TEMP(ENCOUNTER_NUM int,PATIENT_NUM int,INSTANCE_NUM int,CONCEPT_CD varchar(50),START_DATE TIMESTAMP,PROVIDER_ID varchar(50),PANEL_COUNT int,fact_count int,fact_panels int);";
			 String query4 = "CREATE TEMP TABLE DX(ENCOUNTER_NUM int,PATIENT_NUM int,INSTANCE_NUM int,CONCEPT_CD varchar(50),START_DATE TIMESTAMP,PROVIDER_ID varchar(50),temporal_start_date TIMESTAMP,temporal_end_date TIMESTAMP);";
			 String query5 = "insert into QUERY_GLOBAL_TEMP (patient_num, panel_count) with t as (select f.patient_num  from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd  from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\F00-F99\\F30-F39\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD  from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2012-01-01T00:00:00' AND f.start_date <= '2013-01-01T00:00:00') group by  f.patient_num) select t.patient_num, 0 as panel_count from t RETURNING *;";
			 String query6 = "update QUERY_GLOBAL_TEMP  set panel_count = 1 where QUERY_GLOBAL_TEMP.panel_count = 0 and exists (select 1 from (select f.patient_num  from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd  from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\M00-M99\\M50-M54\\M54\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2011-01-01T00:00:00' AND f.start_date <= '2012-01-01T00:00:00') group by f.patient_num) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num);";
			 String query7 = "update QUERY_GLOBAL_TEMP set panel_count = 2 where QUERY_GLOBAL_TEMP.panel_count = 1 and exists (select 1 from (select f.patient_num  from i2b2demodata.observation_fact f where f.provider_id IN (select provider_id  from i2b2demodata.provider_dimension where provider_path LIKE '\\PROVIDER\\ARZT\\10\\%') AND (f.start_date >= '2011-01-01T00:00:00' AND f.start_date <= '2012-01-01T00:00:00') group by  f.patient_num) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num);";
			 String query8 = "update QUERY_GLOBAL_TEMP set panel_count = -1 where QUERY_GLOBAL_TEMP.panel_count = 2 and exists (select 1 from (select f.patient_num  from i2b2demodata.observation_fact f where f.concept_cd IN (select concept_cd from i2b2demodata.concept_dimension where concept_path LIKE '\\ICD\\F00-F99\\F30-F39\\%') AND (f.MODIFIER_CD IN (select MODIFIER_CD from i2b2demodata.MODIFIER_DIMENSION where MODIFIER_PATH LIKE '\\Diagnosesicherheit\\%')) AND (valtype_cd = 'T' AND tval_char IN ('G')) AND (f.start_date >= '2011-01-01T00:00:00' AND f.start_date <= '2012-01-01T00:00:00') group by f.patient_num) t where QUERY_GLOBAL_TEMP.patient_num = t.patient_num);";
			 String query9 = "insert into DX (patient_num) select * from (select distinct patient_num from QUERY_GLOBAL_TEMP where panel_count = 2) q;";
			 String query10 = "select count(distinct patient_num) as patient_num_count from DX;";
			 String query_philipp = "SELECT patient_num, concept_cd_sub, count(*) AS counts FROM (SELECT patient_num, substring(concept_cd FROM 1 FOR 7) AS concept_cd_sub FROM i2b2demodata.observation_fact WHERE concept_cd IN (SELECT concept_cd FROM i2b2demodata.concept_dimension WHERE (concept_cd LIKE 'ATC:%' OR concept_cd LIKE 'ICD:%')) AND (start_date >= '2007-01-01T00:00:00' AND start_date <= '2008-01-01T00:00:00')"/* AND (TRUE = FALSE OR patient_num IN (SELECT patient_num FROM i2b2demodata.qt_patient_set_collection WHERE result_instance_id = 7))*/+") GROUP BY patient_num, concept_cd_sub";
			/* Execute your SQL query */
			 HPCCResultSet res1 = (HPCCResultSet) 
			 stmt.executeQuery(query_philipp);
//			 stmt.executeQuery(query1);
			 
			 /* Check the result set */
			 while(res1.next()){
				 System.out.println(res1.getString(3));
			 }//#OPTION('name', 'java insert');
		
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
