package connectionManagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class ECLLayouts {
	private static final String			Layout_ConceptDimension = "RECORD STRING700 concept_path;  STRING50 concept_cd;  STRING2000 name_char;  STRING concept_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientDimension = "RECORD UNSIGNED5 patient_num;STRING50 vital_status_cd;STRING25 birth_date;STRING25 death_date;STRING50 sex_cd;UNSIGNED2 age_in_years_num;STRING50 language_cd;STRING50 race_cd;STRING50 marital_status_cd;STRING50 religion_cd;STRING10 zip_cd;STRING700 statecityzip_path;STRING50 income_cd;STRING patient_blob;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ObservationFact = "RECORD UNSIGNED5 encounter_num;UNSIGNED5 patient_num;STRING50 concept_cd;STRING50 provider_id;STRING25 start_date;STRING100 modifier_cd;UNSIGNED5 instance_num;STRING50 valtype_cd;STRING255 tval_char;DECIMAL18_5 nval_num;STRING50 valueflag_cd;DECIMAL18_5 quantity_num;STRING50 vunits_cd;STRING25 end_date;STRING50 location_cd;STRING observation_blob;DECIMAL18_5 confidence;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_PatientMapping ="RECORD STRING200 patient_ide;  STRING50 patient_ide_source;  UNSIGNED5 patient_num;  STRING50 patient_ide_status;  STRING50 project_id;  STRING25 upload_date;STRING25 update_date;STRING25 download_date;STRING25 import_date;STRING50 sourcesystem_cd;UNSIGNED5 upload_id;END;";
    private static final String			Layout_ProviderDimension ="RECORD STRING50 provider_id;  STRING700 provider_path;  STRING850 name_char;  STRING provider_blob; STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
    private static final String			Layout_VisitDimension = "RECORD UNSIGNED5 encounter_num;  UNSIGNED5 patient_num;  STRING50 active_status_cd;  STRING25 start_date;  STRING25 end_date;  STRING50 inout_cd;  STRING50 location_cd;  STRING900 location_path;  UNSIGNED5 length_of_stay;  STRING visit_blob;  STRING25 update_date;  STRING25 download_date;  STRING25 import_date;  STRING50 sourcesystem_cd;  UNSIGNED5 upload_id;END;";
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
			layouts.put("test", "RECORD unsigned5 nummer; END;");
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
	
	public static TreeSet<String> getAllColumns(String table) {
		TreeSet<String> allColumns = new TreeSet<String>();
		String[] columns = getLayouts().get(table).split(";");
		for (String l : columns) {
			if (l.matches(".*END")) continue;
			String[] entries = l.split(" ");
			allColumns.add(entries[entries.length-1]);
		}	
		return allColumns;
	}

}
