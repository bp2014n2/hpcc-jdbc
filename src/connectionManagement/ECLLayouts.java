package connectionManagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

public class ECLLayouts {
	private static final String			Layout_ConceptDimension = "RECORD STRING700 concept_path;  STRING50 concept_cd;  STRING2000 name_char;  STRING concept_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientDimension = "RECORD UNSIGNED5 patient_num;STRING50 vital_status_cd;STRING25 birth_date;STRING25 death_date;STRING50 sex_cd;UNSIGNED2 age_in_years_num;STRING50 language_cd;STRING50 race_cd;STRING50 marital_status_cd;STRING50 religion_cd;STRING10 zip_cd;STRING700 statecityzip_path;STRING50 income_cd;STRING patient_blob;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ObservationFact = "RECORD UNSIGNED5 encounter_num;UNSIGNED5 patient_num;STRING50 concept_cd;STRING50 provider_id;STRING25 start_date;STRING100 modifier_cd;UNSIGNED5 instance_num;STRING50 valtype_cd;STRING255 tval_char;DECIMAL18_5 nval_num;STRING50 valueflag_cd;DECIMAL18_5 quantity_num;STRING50 vunits_cd;STRING25 end_date;STRING50 location_cd;STRING observation_blob;DECIMAL18_5 confidence;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientMapping ="RECORD STRING200 patient_ide;  STRING50 patient_ide_source;  UNSIGNED5 patient_num;  STRING50 patient_ide_status;  STRING50 project_id;  STRING25 upload_date;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ProviderDimension ="RECORD STRING50 provider_id;  STRING700 provider_path;  STRING850 name_char;  STRING provider_blob; STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_VisitDimension = "RECORD UNSIGNED5 encounter_num;  UNSIGNED5 patient_num;  STRING50 active_status_cd;  STRING25 start_date;  STRING25 end_date;  STRING50 inout_cd;  STRING50 location_cd;  STRING900 location_path;  UNSIGNED5 length_of_stay;  STRING visit_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_ArchiveObservationFact = "RECORD UNSIGNED5 encounter_num; UNSIGNED5 patient_num; STRING50 concept_cd; STRING50 provider_id; STRING25 start_date; STRING100 modifier_cd; UNSIGNED5 instance_num; STRING50 valtype_cd; STRING255 tval_char; DECIMAL18_5 nval_num; STRING50 valueflag_cd; DECIMAL18_5 quantity_num; STRING50 vunits_cd; STRING25 end_date; STRING50 location_cd; STRING observation_blob; DECIMAL18_5 confidence; STRING25 update_date; STRING25 download_date; STRING25 import_date; STRING50 sourcesystem_cd; UNSIGNED5 upload_id; END;";
    private static final String			Layout_CodeLookup = "RECORD STRING100 table_cd; STRING100 column_cd; STRING50 code_cd; STRING650 name_char; STRING lookup_blob; STRING25 upload_date; STRING25 update_date; STRING25 download_date; STRING25 import_date; STRING50 sourcesystem_cd; UNSIGNED5 upload_id; END;";
    private static final String			Layout_DatamartReport = "RECORD UNSIGNED5 total_patient; UNSIGNED5 total_observationfact; UNSIGNED5 total_event; STRING25 report_date;END;";
    private static final String			Layout_EncounterMapping = "RECORD STRING200 encounter_ide; STRING50 encounter_ide_source; STRING50 project_id; UNSIGNED5 encounter_num; STRING200 patient_ide; STRING50 patient_ide_source; STRING50 encounter_ide_status; STRING25 upload_date; STRING25 update_date; STRING25 download_date; STRING25 import_date; STRING50 sourcesystem_cd; UNSIGNED5 upload_id; END;";
    private static final String			Layout_ModifierDimension = "RECORD STRING700 modifier_path; STRING50 modifier_cd; STRING2000 name_char; STRING modifier_blob; STRING25 update_date; STRING25 download_date; STRING25 import_date; STRING50 sourcesystem_cd; UNSIGNED5 upload_id; END;";
    private static final String			Layout_QtAnalysisPlugin = "RECORD UNSIGNED5 plugin_id; STRING2000 plugin_name; STRING2000 description; STRING50 version_cd; STRING parameter_info; STRING parameter_info_xsd; STRING command_line; STRING working_folder; STRING commandoption_cd; STRING plugin_icon; STRING50 status_cd; STRING50 user_id; STRING50 group_id; STRING25 create_date; STRING25 update_date; END;";
    private static final String			Layout_QtAnalysisPluginResultType = "qt_analysis_plugin_result_type := RECORD UNSIGNED5 plugin_id; UNSIGNED5 result_type_id; END;";
    private static final String			Layout_QtBreakdownPath = "RECORD STRING100 name; STRING2000 value; STRING25 create_date; STRING25 update_date; STRING50 user_id; END;";
    private static final String			Layout_QtPatientEncCollection = "RECORD UNSIGNED5 patient_enc_coll_id; UNSIGNED5 result_instance_id; UNSIGNED5 set_index; UNSIGNED5 patient_num; UNSIGNED5 encounter_num; END;";
    private static final String			Layout_QtPatientSetCollection = "RECORD UNSIGNED patient_set_coll_id; UNSIGNED5 result_instance_id; UNSIGNED5 set_index; UNSIGNED5 patient_num; END;";
    private static final String			Layout_QtPdoQueryMaster = "RECORD UNSIGNED5 query_master_id; STRING50 user_id; STRING50 group_id; STRING25 create_date; STRING request_xml; STRING i2b2_request_xml; END;";
    private static final String			Layout_QtPrivilege = "RECORD STRING1500 protection_label_cd; STRING1000 dataprot_cd; STRING1000 hivemgmt_cd; UNSIGNED5 plugin_id; END;";
    private static final String			Layout_QtQueryInstance = "RECORD UNSIGNED5 query_instance_id; UNSIGNED5 query_master_id; STRING50 user_id; STRING50 group_id; STRING50 batch_mode; STRING25 start_date; STRING25 end_date; STRING3 delete_flag; UNSIGNED5 status_type_id; STRING message; END;";
    private static final String			Layout_QtQueryMaster = "RECORD UNSIGNED5 query_master_id; STRING250 name; STRING50 user_id; STRING50 group_id; STRING2000 master_type_cd; UNSIGNED5 plugin_id; STRING25 create_date; STRING25 delete_date; STRING3 delete_flag; STRING request_xml; STRING generated_sql; STRING i2b2_request_xml; END;";
    private static final String			Layout_QtQueryResultInstance = "RECORD UNSIGNED5 result_instance_id; UNSIGNED5 query_instance_id; UNSIGNED5 result_type_id; UNSIGNED5 set_size; STRING25 start_date; STRING25 end_date; UNSIGNED5 status_type_id; STRING3 delete_flag; STRING message; STRING200 description; UNSIGNED5 real_set_size; STRING500 obfusc_method; END;";
    private static final String			Layout_QtQueryResultType = "RECORD UNSIGNED5 result_type_id; STRING100 name; STRING200 description; STRING500 display_type_id; STRING3 visual_attribute_type_id; END;";
    private static final String			Layout_QtQueryStatusType = "RECORD UNSIGNED5 status_type_id; STRING100 name; STRING200 description; END;";
    private static final String			Layout_QtXmlResult = "RECORD UNSIGNED5 xml_result_id; UNSIGNED5 result_instance_id; STRING xml_value; END;";
    private static final String			Layout_SetType = "RECORD UNSIGNED5 id; STRING500 name; STRING25 create_date; END;";
    private static final String			Layout_SetUploadStatus = "RECORD UNSIGNED5 upload_id; UNSIGNED5 set_type_id; STRING50 source_cd; UNSIGNED no_of_record; UNSIGNED loaded_record; UNSIGNED deleted_record; STRING25 load_date; STRING25 end_date; STRING100 load_status; STRING message; STRING input_file_name; STRING log_file_name; STRING500 transform_name; END;";
    private static final String			Layout_SourceMaster = "RECORD STRING50 source_cd; STRING300 description; STRING25 create_date; END;";
    private static final String			Layout_UploadStatus = "RECORD UNSIGNED5 upload_id; STRING500 upload_label; STRING100 user_id; STRING50 source_cd; UNSIGNED no_of_record; UNSIGNED loaded_record; UNSIGNED deleted_record; STRING25 load_date; STRING25 end_date; STRING100 load_status; STRING message; STRING input_file_name; STRING log_file_name; STRING500 transform_name; END;";
    private static final String			Layout_QueryGlobalTemp = "RECORD UNSIGNED5 encounter_num; UNSIGNED5 patient_num; UNSIGNED5 instance_num; STRING50 concept_cd; STRING25 start_date; STRING50 provider_id; UNSIGNED5 panel_count; UNSIGNED5 fact_count; UNSIGNED5 fact_panels; END;";
    private static final String			Layout_Dx = "RECORD UNSIGNED5 encounter_num; UNSIGNED5 patient_num; UNSIGNED5 instance_num; STRING50 concept_cd; STRING25 start_date; STRING50 provider_id; STRING25 temporal_start_date; STRING25 temporal_end_date; END;";
    
    private static HashMap<String, String> layouts = new HashMap<String, String> ();
     
    /**
	 * 
	 * @return returns a HashMap with all layouts from i2b2demodata referenced by the table name
	 */
	public static HashMap<String, String> getLayouts() {
		if (layouts.isEmpty()) {
			layouts.put("concept_dimension", Layout_ConceptDimension);
			layouts.put("patient_dimension", Layout_PatientDimension);
			layouts.put("observation_fact", Layout_ObservationFact);
			layouts.put("patient_mapping", Layout_PatientMapping);
			layouts.put("provider_dimension", Layout_ProviderDimension);
			layouts.put("visit_dimension", Layout_VisitDimension);
			layouts.put("archive_observation_fact", Layout_ArchiveObservationFact);
			layouts.put("code_lookup", Layout_CodeLookup);
			layouts.put("datamart_report", Layout_DatamartReport);
			layouts.put("encounter_mapping", Layout_EncounterMapping);
			layouts.put("modifier_dimension", Layout_ModifierDimension);
			layouts.put("qt_analysis_plugin", Layout_QtAnalysisPlugin);
			layouts.put("qt_analysis_plugin_result_type", Layout_QtAnalysisPluginResultType);
			layouts.put("qt_breakdown_path", Layout_QtBreakdownPath);
			layouts.put("qt_patient_enc_collection", Layout_QtPatientEncCollection);
			layouts.put("qt_patient_set_collection", Layout_QtPatientSetCollection);
			layouts.put("qt_pdo_query_master", Layout_QtPdoQueryMaster);
			layouts.put("qt_privilege", Layout_QtPrivilege);
			layouts.put("qt_query_instance", Layout_QtQueryInstance);
			layouts.put("qt_query_master", Layout_QtQueryMaster);
			layouts.put("qt_query_result_instance", Layout_QtQueryResultInstance);
			layouts.put("qt_query_result_type", Layout_QtQueryResultType);
			layouts.put("qt_query_status_type", Layout_QtQueryStatusType);
			layouts.put("qt_xml_result", Layout_QtXmlResult);
			layouts.put("set_type", Layout_SetType);
			layouts.put("set_upload_status", Layout_SetUploadStatus);
			layouts.put("source_master", Layout_SourceMaster);
			layouts.put("upload_status", Layout_UploadStatus);
			layouts.put("query_global_temp", Layout_QueryGlobalTemp);
			layouts.put("dx", Layout_Dx);
		}
		return layouts;
	}
	
	/**
	 * is only used within tests to add test layouts
	 * @param key is the name of the corresponding table
	 * @param value is the layout definition itself
	 */
	public static void setLayouts(String key, String value) {
		layouts.put(key, value);
	}
	
	public static String getECLDataType(String table, String column){
		String dataType = "unknown";
		String[] columns = layouts.get(table).split(";");
		for(String l : columns) {
			if (!l.matches(".*"+column)) continue;
			String[] entries = l.split(" ");
			dataType = entries[entries.length-2];
			break;
		}
		return dataType;
	}
	
	public static LinkedHashSet<String> getAllColumns(String table) {
		LinkedHashSet<String> allColumns = new LinkedHashSet<String>();
		String[] columns = layouts.get(table).split(";");
		for (String l : columns) {
			if (l.matches(".*END")) continue;
			String[] entries = l.split(" ");
			allColumns.add(entries[entries.length-1]);
		}	
		return allColumns;
	}

}
