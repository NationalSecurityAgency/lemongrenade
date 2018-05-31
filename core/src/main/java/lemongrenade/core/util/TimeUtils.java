package lemongrenade.core.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimeUtils {

    //Takes an iso8601 formatted string and returns a Date object
    static public Date parseIso8601(String input) throws Exception {
        DateFormat iso8601;
        TimeZone tz = TimeZone.getTimeZone("UTC");
        if(input.length() == 17) //"2017-05-31T11:42Z"
            iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");// +'Z'
        else{
            iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");// +'Z'
        }
        iso8601.setTimeZone(tz);
        Date myDate = new Date();
        try {
            myDate = iso8601.parse(input);
            return myDate;
        } catch (ParseException e) {
            myDate.setTime(Long.valueOf(input));
            return myDate;
        }
    }

    static public String getIsoString(Date input) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");//'Z' indicates UTC
        df.setTimeZone(tz);
        String nowAsISO = df.format(input);
        return nowAsISO;
    }

    static public Long getEpoch(Date input) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(input);
        return cal.getTimeInMillis();
    }
}