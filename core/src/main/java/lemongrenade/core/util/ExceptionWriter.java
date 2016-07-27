package lemongrenade.core.util;

import org.apache.storm.shade.org.apache.commons.lang.SerializationUtils;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;

public class ExceptionWriter extends GetDeadLetterMessages {

    public ExceptionWriter() throws Exception {
        super("Exceptions");
    }

//    public void publishException(Exception e) {
//        try {
//            channel.basicPublish("Exceptions", "", null, SerializationUtils.serialize(e));
//        } catch (Exception e1) {
//            e1.printStackTrace();
//        }
//    }

    public void publishException(String taskId, String requests, Exception e) {
        try {
            HashMap<String, String> map = new HashMap();
            map.put("taskId", taskId);
            map.put("requests", requests);
            map.put("cause", e.toString());
            map.put("detailmessage", e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            map.put("stackTrace", sw.toString());
            channel.basicPublish("Exceptions", "", null, SerializationUtils.serialize(map));
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public void printBody(byte[] body) {
        try {
            HashMap<String, String> map;
            //Exception exception = (Exception) SerializationUtils.deserialize(body);
            map = (HashMap) SerializationUtils.deserialize(body);
            System.out.println(map.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ExceptionWriter DLM = new ExceptionWriter();
        DLM.processErrors(100); //print and republish up to 100 errors from DeadLetter queue
        System.out.println("Done");
        System.exit(1); //closes consumer
    }
}
