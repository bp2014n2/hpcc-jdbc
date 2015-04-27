/*##############################################################################

Copyright (C) 2011 HPCC Systems.

All rights reserved. This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

package de.hpi.hpcc.main;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.hpi.hpcc.logging.HPCCLogger;

public class HPCCLogicalFiles
{
    private Properties   files;
    private List<String> superfiles;
    private long         reportedFileCount;
    private static final Logger logger = HPCCLogger.getLogger();

    public HPCCLogicalFiles()
    {
        files = new Properties();
        superfiles = new ArrayList<String>();

        reportedFileCount = 0;
    }

    public void putFile(String fullyQualifiedName, HPCCDFUFile file)
    {
        files.put(fullyQualifiedName.toUpperCase(), file);
        if (file.isSuperFile())
            superfiles.add(fullyQualifiedName);
    }

    public void putFile(HPCCDFUFile file)
    {
        files.put(file.getFullyQualifiedName().toUpperCase(), file);
        if (file.isSuperFile())
            superfiles.add(file.getFullyQualifiedName());
    }

    public void removeFile(String fullyQualifiedName) {
    	files.remove(fullyQualifiedName.toUpperCase());
    	superfiles.remove(fullyQualifiedName.toLowerCase());
    }
    
    public boolean containsFileName(String filename)
    {
        return files.containsKey(filename.toUpperCase());
    }

    public HPCCDFUFile getFile(String filename)
    {
        return (HPCCDFUFile) files.get(filename.toUpperCase());
    }

    public Enumeration<Object> getFiles()
    {
        return files.elements();
    }

    private String getSubfileRecDef(HPCCDFUFile superfile)
    {
        String eclrecdef = "";

        List<String> subfiles = superfile.getSubfiles();
        for (int y = 0; y < subfiles.size(); y++)
        {
            HPCCDFUFile subfile = ((HPCCDFUFile) files.get(subfiles.get(y).toUpperCase()));
            if (subfile != null)
            {
                if (subfile.hasFileRecDef())
                {
                    eclrecdef = subfile.getFileRecDef("recdef");
                    log("\tUsing record definition from: " + subfile.getFullyQualifiedName());
                    break;
                }
                else if (subfile.isSuperFile())
                {
                    eclrecdef = getSubfileRecDef(subfile);
                    if (!eclrecdef.equals(""))
                        break;
                }
            }
        }
        return eclrecdef;
    }

    public void updateSuperFile(String superfilename)
    {
        HPCCDFUFile superfile = (HPCCDFUFile) files.get(superfilename.toUpperCase());
        if (!superfile.hasFileRecDef())
        {
            if (superfile.containsSubfiles())
            {
                log("Processing superfile: " + superfile.getFullyQualifiedName());
                superfile.setFileRecDef(getSubfileRecDef(superfile));
                files.put(superfile.getFullyQualifiedName().toUpperCase(), superfile);
            }
        }
    }

    public void updateSuperFiles()
    {
        int superfilescount = superfiles.size();
        int superfilesupdated = 0;

        for (int i = 0; i < superfilescount; i++)
        {
            HPCCDFUFile superfile = (HPCCDFUFile) files.get(superfiles.get(i).toUpperCase());
            if (!superfile.hasFileRecDef())
            {
                if (superfile.containsSubfiles())
                {
                    log("Processing superfile: " + superfile.getFullyQualifiedName());
                    superfile.setFileRecDef(getSubfileRecDef(superfile));
                    if (superfile.hasFileRecDef())
                    {
                        files.put(superfile.getFullyQualifiedName().toUpperCase(), superfile);
                        superfilesupdated++;
                    }
                }
            }
        }
        log("Update superfiles' record definition ( " + superfilesupdated + " out of " + superfilescount + " )");
    }

    public long getReportedFileCount()
    {
        return reportedFileCount;
    }

    public void setReportedFileCount(long reportedFileCount)
    {
        this.reportedFileCount = reportedFileCount;
    }

    public void setReportedFileCount(String reportedFileCountStr)
    {
        this.reportedFileCount = HPCCJDBCUtils.stringToLong(reportedFileCountStr, 0);
    }

    public int getCachedFileCount()
    {
        return files.size();
    }

    public int getCachedSuperFileCount()
    {
        return superfiles.size();
    }
    
    //Logger methods
	private static void log(String infoMessage){
		log(Level.INFO, infoMessage);
	}
	
	private static void log(Level loggingLevel, String infoMessage){
		logger.log(loggingLevel, HPCCLogicalFiles.class.getSimpleName()+": "+infoMessage);
	}
}
