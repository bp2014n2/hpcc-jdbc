package de.hpi.hpcc.logging;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import de.hpi.hpcc.main.HPCCJDBCUtils;

public class HPCCLogFormatter  extends Formatter
{
    private static final ThreadLocal<DateFormat> DF =
            new ThreadLocal<DateFormat>()
            {
                @Override
                protected DateFormat initialValue()
                {
                  return new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSS");
                }
            };

    private static final String THREADFORMAT = "%05d";

    @Override
    public String format(LogRecord arg0)
    {
        StringBuilder builder = new StringBuilder();

        builder.append("["+arg0.getLevel()+"]")
        	   .append(" ")
        	   .append(DF.get().format(new Date(arg0.getMillis())))
               .append(" ")
               .append(String.format(THREADFORMAT, Thread.currentThread().getId()))
               .append(" ")
               .append(formatMessage(arg0))
               .append(HPCCJDBCUtils.newLine);

        return builder.toString();
    }
}
