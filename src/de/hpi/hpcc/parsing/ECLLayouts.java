package de.hpi.hpcc.parsing;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import de.hpi.hpcc.parsing.ECLRecordDefinition;

public class ECLLayouts {
	private static final String			Layout_ConceptDimension = "concept_path character varying(700) NOT NULL, concept_cd character varying(50), name_char character varying(2000), concept_blob text, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer";
    private static final String			Layout_PatientDimension = "patient_num integer NOT NULL, vital_status_cd character varying(50), birth_date timestamp without time zone, death_date timestamp without time zone, sex_cd character varying(50), age_in_years_num integer, language_cd character varying(50), race_cd character varying(50), marital_status_cd character varying(50), religion_cd character varying(50), zip_cd character varying(10), statecityzip_path character varying(700), income_cd character varying(50), patient_blob text, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer";
    private static final String			Layout_ObservationFact = "encounter_num integer NOT NULL, patient_num integer NOT NULL, concept_cd character varying(50) NOT NULL, provider_id character varying(50) NOT NULL, start_date timestamp without time zone NOT NULL, modifier_cd character varying(100) NOT NULL DEFAULT '@'::character varying, instance_num integer NOT NULL DEFAULT 1, valtype_cd character varying(50), tval_char character varying(255), nval_num int, valueflag_cd character varying(50), quantity_num int, units_cd character varying(50), end_date timestamp without time zone, location_cd character varying(50), observation_blob text, confidence_num int, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer, text_search_index serial NOT NULL";
    private static final String			Layout_PatientMapping ="patient_ide character varying(200) NOT NULL, patient_ide_source character varying(50) NOT NULL, patient_num integer NOT NULL, patient_ide_status character varying(50), project_id character varying(50) NOT NULL, upload_date timestamp without time zone, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer";
    private static final String			Layout_ProviderDimension ="provider_id character varying(50) NOT NULL, provider_path character varying(700) NOT NULL, name_char character varying(850), provider_blob text, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer";
    private static final String			Layout_VisitDimension = "encounter_num integer NOT NULL, patient_num integer NOT NULL, active_status_cd character varying(50), start_date timestamp without time zone, end_date timestamp without time zone, inout_cd character varying(50), location_cd character varying(50), location_path character varying(900), length_of_stay integer, visit_blob text, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer, age_in_years integer, treatment integer";
    private static final String			Layout_ArchiveObservationFact = "encounter_num integer, patient_num integer, concept_cd character varying(50), provider_id character varying(50), start_date timestamp without time zone, modifier_cd character varying(100), instance_num integer, valtype_cd character varying(50), tval_char character varying(255), nval_num int, valueflag_cd character varying(50), quantity_num int, units_cd character varying(50), end_date timestamp without time zone, location_cd character varying(50), observation_blob text, confidence_num int, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer, text_search_index integer, archive_upload_id integer";
    private static final String			Layout_CodeLookup = "table_cd character varying(100) NOT NULL, column_cd character varying(100) NOT NULL, code_cd character varying(50) NOT NULL, name_char character varying(650), lookup_blob text, upload_date timestamp without time zone, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer";
    private static final String			Layout_DatamartReport = "total_patient integer, total_observationfact integer, total_event integer, report_date timestamp without time zone";
    
    //may needs refactoring due to translation of ECL Layout to SQL definition without caring for original SQL definition
    private static final String			Layout_EncounterMapping = "encounter_ide varchar(200), encounter_ide_source varchar(50), project_id varchar(50), encounter_num int, patient_ide varchar(200), patient_ide_source varchar(50), encounter_ide_status varchar(50), upload_date timestamp, update_date timestamp, download_date timestamp, import_date timestamp, sourcesystem_cd varchar(50), upload_id int";
    
    private static final String			Layout_ModifierDimension = "modifier_path character varying(700) NOT NULL, modifier_cd character varying(50), name_char character varying(2000), modifier_blob text, update_date timestamp without time zone, download_date timestamp without time zone, import_date timestamp without time zone, sourcesystem_cd character varying(50), upload_id integer";
    private static final String			Layout_QtAnalysisPlugin = "plugin_id integer NOT NULL, plugin_name character varying(2000), description character varying(2000), version_cd character varying(50), parameter_info text, parameter_info_xsd text, command_line text, working_folder text, commandoption_cd text, plugin_icon text, status_cd character varying(50), user_id character varying(50), group_id character varying(50), create_date timestamp without time zone, update_date timestamp without time zone";
    private static final String			Layout_QtAnalysisPluginResultType = "plugin_id integer NOT NULL, result_type_id integer NOT NULL";
    private static final String			Layout_QtBreakdownPath = "name character varying(100), value character varying(2000), create_date timestamp without time zone, update_date timestamp without time zone, user_id character varying(50)";
    private static final String			Layout_QtPatientEncCollection = "patient_enc_coll_id serial NOT NULL, result_instance_id integer, set_index integer, patient_num integer, encounter_num integer";
    private static final String			Layout_QtPatientSetCollection = "patient_set_coll_id bigserial NOT NULL, result_instance_id integer, set_index integer, patient_num integer";
    private static final String			Layout_QtPdoQueryMaster = "query_master_id serial NOT NULL, user_id character varying(50) NOT NULL, group_id character varying(50) NOT NULL, create_date timestamp without time zone NOT NULL, request_xml text, i2b2_request_xml text";
    private static final String			Layout_QtPrivilege = "protection_label_cd character varying(1500), dataprot_cd character varying(1000), hivemgmt_cd character varying(1000), plugin_id integer";
    private static final String			Layout_QtQueryInstance = "query_instance_id serial NOT NULL, query_master_id integer, user_id character varying(50) NOT NULL, group_id character varying(50) NOT NULL, batch_mode character varying(50), start_date timestamp without time zone NOT NULL, end_date timestamp without time zone, delete_flag character varying(3), status_type_id integer, message text";
    private static final String			Layout_QtQueryMaster = "query_master_id serial NOT NULL, name character varying(250) NOT NULL, user_id character varying(50) NOT NULL, group_id character varying(50) NOT NULL, master_type_cd character varying(2000), plugin_id integer, create_date timestamp without time zone NOT NULL, delete_date timestamp without time zone, delete_flag character varying(3), request_xml text, generated_sql text, i2b2_request_xml text, pm_xml text";
    private static final String			Layout_QtQueryResultInstance = "result_instance_id serial NOT NULL, query_instance_id integer, result_type_id integer NOT NULL, set_size integer, start_date timestamp without time zone NOT NULL, end_date timestamp without time zone, status_type_id integer NOT NULL, delete_flag character varying(3), message text, description character varying(200), real_set_size integer, obfusc_method character varying(500)";
    private static final String			Layout_QtQueryResultType = "result_type_id integer NOT NULL, name character varying(100), description character varying(200), display_type_id character varying(500), visual_attribute_type_id character varying(3)";
    private static final String			Layout_QtQueryStatusType = "status_type_id integer NOT NULL, name character varying(100), description character varying(200)";
    private static final String			Layout_QtXmlResult = "xml_result_id serial NOT NULL, result_instance_id integer, xml_value text";
    private static final String			Layout_SetType = "id integer NOT NULL, name character varying(500), create_date timestamp without time zone";
    private static final String			Layout_SetUploadStatus = "upload_id integer NOT NULL, set_type_id integer NOT NULL, source_cd character varying(50) NOT NULL, no_of_record bigint, loaded_record bigint, deleted_record bigint, load_date timestamp without time zone NOT NULL, end_date timestamp without time zone, load_status character varying(100), message text, input_file_name text, log_file_name text, transform_name character varying(500)";
    private static final String			Layout_SourceMaster = "source_cd character varying(50) NOT NULL, description character varying(300), create_date timestamp without time zone";
    private static final String			Layout_UploadStatus = "upload_id serial NOT NULL, upload_label character varying(500) NOT NULL, user_id character varying(100) NOT NULL, source_cd character varying(50) NOT NULL, no_of_record bigint, loaded_record bigint, deleted_record bigint, load_date timestamp without time zone NOT NULL, end_date timestamp without time zone, load_status character varying(100), message text, input_file_name text, log_file_name text, transform_name character varying(500)";
    private static final String			Layout_QueryGlobalTemp = "encounter_num int, patient_num int, instance_num int, concept_cd varchar(50), start_date TIMESTAMP, provider_id varchar(50), panel_count int, fact_count int, fact_panels int";
    private static final String			Layout_Dx = "encounter_num int, patient_num int, instance_num int, concept_cd varchar(50), start_date timestamp, provider_id varchar(50), temporal_start_date timestamp, temporal_end_date timestamp";
    private static final String			Layout_Sequences = "name varchar(50), value int(8), start int(8)";
    
    private static HashMap<String, ECLRecordDefinition> layouts = new HashMap<String, ECLRecordDefinition> ();
     
    /**
	 * 
	 * @return returns a HashMap with all layouts from i2b2demodata referenced by the table name
	 */
	public static HashMap<String, ECLRecordDefinition> getLayouts() {
		if (layouts.isEmpty()) {
			layouts.put("concept_dimension", new ECLRecordDefinition(Layout_ConceptDimension));
			layouts.put("patient_dimension", new ECLRecordDefinition(Layout_PatientDimension));
			layouts.put("observation_fact", new ECLRecordDefinition(Layout_ObservationFact));
			layouts.put("patient_mapping", new ECLRecordDefinition(Layout_PatientMapping));
			layouts.put("provider_dimension", new ECLRecordDefinition(Layout_ProviderDimension));
			layouts.put("visit_dimension", new ECLRecordDefinition(Layout_VisitDimension));
			layouts.put("archive_observation_fact", new ECLRecordDefinition(Layout_ArchiveObservationFact));
			layouts.put("code_lookup", new ECLRecordDefinition(Layout_CodeLookup));
			layouts.put("datamart_report", new ECLRecordDefinition(Layout_DatamartReport));
			layouts.put("encounter_mapping", new ECLRecordDefinition(Layout_EncounterMapping));
			layouts.put("modifier_dimension", new ECLRecordDefinition(Layout_ModifierDimension));
			layouts.put("qt_analysis_plugin", new ECLRecordDefinition(Layout_QtAnalysisPlugin));
			layouts.put("qt_analysis_plugin_result_type", new ECLRecordDefinition(Layout_QtAnalysisPluginResultType));
			layouts.put("qt_breakdown_path", new ECLRecordDefinition(Layout_QtBreakdownPath));
			layouts.put("qt_patient_enc_collection", new ECLRecordDefinition(Layout_QtPatientEncCollection));
			layouts.put("qt_patient_set_collection", new ECLRecordDefinition(Layout_QtPatientSetCollection));
			layouts.put("qt_pdo_query_master", new ECLRecordDefinition(Layout_QtPdoQueryMaster));
			layouts.put("qt_privilege", new ECLRecordDefinition(Layout_QtPrivilege));
			layouts.put("qt_query_instance", new ECLRecordDefinition(Layout_QtQueryInstance));
			layouts.put("qt_query_master", new ECLRecordDefinition(Layout_QtQueryMaster));
			layouts.put("qt_query_result_instance", new ECLRecordDefinition(Layout_QtQueryResultInstance));
			layouts.put("qt_query_result_type", new ECLRecordDefinition(Layout_QtQueryResultType));
			layouts.put("qt_query_status_type", new ECLRecordDefinition(Layout_QtQueryStatusType));
			layouts.put("qt_xml_result", new ECLRecordDefinition(Layout_QtXmlResult));
			layouts.put("set_type", new ECLRecordDefinition(Layout_SetType));
			layouts.put("set_upload_status", new ECLRecordDefinition(Layout_SetUploadStatus));
			layouts.put("source_master", new ECLRecordDefinition(Layout_SourceMaster));
			layouts.put("upload_status", new ECLRecordDefinition(Layout_UploadStatus));
			layouts.put("query_global_temp", new ECLRecordDefinition(Layout_QueryGlobalTemp));
			layouts.put("dx", new ECLRecordDefinition(Layout_Dx));
			layouts.put("sequences", new ECLRecordDefinition(Layout_Sequences));
		}
		return layouts;
	}
	
	/**
	 * is only used within tests to add test layouts
	 * @param key is the name of the corresponding table
	 * @param value is the layout definition itself
	 */
	public static void setLayouts(String key, String value) {
		layouts.put(key.toLowerCase(), new ECLRecordDefinition(value));
	}
	
	public static String getECLDataType(String table, String column){
		String dataType = "unknown";
	    
		ECLRecordDefinition recordDefinition = getLayouts().get(table.toLowerCase());
		return recordDefinition.findColumn(column).getEclDataType();
	}
	
	public static LinkedHashSet<String> getAllColumns(String table) {
		ECLRecordDefinition recordDefinition = getLayouts().get(table.toLowerCase());
		
		return recordDefinition.getColumnNames();
	}

	
	public static boolean isInt(ECLRecordDefinition layout, String column) {	
		if (layout.findColumn(column) != null && layout.findColumn(column).getEclDataType().toLowerCase().matches("(unsigned.*|integer.*)")) {
			return true;
		}
		return false;
	}
	
	public static boolean isColumnOfIntInAnyTable(String column) {
		for (ECLRecordDefinition table : getLayouts().values()) {
			if (isInt(table, column)) return true;
		}
		return false;
	}
	
	public static String getSQLTypeOfColumn (String column) {
		for (ECLRecordDefinition table : getLayouts().values()) {
			ECLColumnDefinition columnDefinition = table.findColumn(column);
			if (columnDefinition != null) {
				return columnDefinition.getSqlDataType();
			}
		}
		return "";
	}

}
