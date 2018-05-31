package lemongrenade.core.models;

import lemongrenade.core.util.LGProperties;
import lemongrenade.core.util.ReadFile;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static lemongrenade.core.util.ReadFile.getContent;


public class LGPayloadTest {
    private static final Logger log = LoggerFactory.getLogger(LGPayloadTest.class);

    public static void writeObject(LGPayload payload) throws Exception {
        FileOutputStream tempOutStream = new FileOutputStream("/data/test_files/temp.txt");
        java.io.ObjectOutputStream out = new ObjectOutputStream(tempOutStream);
        try {
            log.info("Starting writeObject.");
            //Writes the payload to the output stream
            Method writeObject = payload.getClass().getDeclaredMethod("writeObject", new Class<?>[]{ObjectOutputStream.class});
            writeObject.setAccessible(true);
            writeObject.invoke(payload, out);
            out.flush();
            out.close();
        }
        catch(Exception e) {
            out.flush();
            out.close();
            throw e;
        }
    }

    public static void readObject(LGPayload payload) throws Exception {
        FileInputStream tempInStream = new FileInputStream("/data/test_files/temp.txt");
        java.io.ObjectInputStream in = new ObjectInputStream(tempInStream);
        try {
            log.info("Starting readObject");
            //Reads the payload from the input stream
            Method readObject = payload.getClass().getDeclaredMethod("readObject", new Class<?>[]{ObjectInputStream.class});
            readObject.setAccessible(true);
            readObject.invoke(payload, in);
            in.close();
            log.info("Done.");
        }
        catch(Exception e) {
            in.close();
            throw e;
        }
    }

    //This test passes for reading in a large amount of memory
    public void writeLargeMemoryTest() throws Exception {
        FileInputStream contentIn = new FileInputStream("/data/test_files/1MBtest.txt");
        LGPayload payload;
        try {
            String job_id = "1";
            String task_id = "2";
            JSONObject job_config = new JSONObject()
                    .put("job_config", "job_config")
                    ;
            JSONObject responses = new JSONObject()
                    .put("nodes", new JSONArray())
                    .put("edges", new JSONArray())
                    .put("responses", "responses")
                    ;
            String content = getContent(contentIn);

            payload = new LGPayload(job_id, task_id, job_config, responses);
            payload.addResponseNode(new JSONObject().put("response_item", "response_value"));
            JSONObject data = new JSONObject()
                    .put("content", content)
                    ;
            for(int i = 0; i < 100; i++) {
                if(i%10 == 0) {
                    log.info("i:" + i);
                }
                payload.addResponseNode(data);
            }
            contentIn.close();
        } catch(Exception e) {
            contentIn.close();
            e.printStackTrace();
            throw e;
        }
        writeObject(payload);
        readObject(payload);
    }

    @Test public void writeBasicTest() throws Exception {
        LGPayload payload1 = new LGPayload();
        payload1.setJobId("1");
        payload1.setTaskId("2");
        JSONObject requestNode = new JSONObject()
                .put("type", "requestNode")
                .put("value", "value")
                .put("requestObject", new JSONObject().put("object", new JSONArray()))
                ;
        JSONObject responseNode = new JSONObject()
                .put("type", "responseNode")
                .put("value", "value")
                .put("responseObject", new JSONObject().put("object", new JSONArray()))
                ;
        JSONObject edge = new JSONObject()
                .put("type", "edge")
                .put("value", "value")
                ;
        JSONObject job_config = new JSONObject()
                .put("test_job_config", "test")
                ;
        payload1.setJobConfig(job_config);
        payload1.addRequestNode(requestNode);
        payload1.addResponseNode(responseNode);
        payload1.addRequestEdge(requestNode, edge, requestNode);
        payload1.addResponseEdge(responseNode, edge, responseNode);
        writeObject(payload1);
        LGPayload payload2 = new LGPayload();
        readObject(payload2);
        List<JSONObject> requestNodes = payload2.getRequestNodes();
        assert requestNodes.size() == 1;
        requestNode = requestNodes.get(0);
        assert requestNode.has("requestObject");
        assert requestNode.getJSONObject("requestObject").has("object");
        List<JSONObject> responseNodes = payload2.getResponseNodes();
        assert responseNodes.size() == 1;
        List<JSONArray> requestEdges = payload2.getRequestEdges();
        assert requestEdges.size() == 1;
        assert requestEdges.get(0).length() == 3;
        assert requestEdges.get(0).getJSONObject(0).has("requestObject");
        List<JSONArray> responseEdges = payload2.getResponseEdges();
        assert responseEdges.size() == 1;
        assert responseEdges.get(0).length() == 3;
        assert responseEdges.get(0).getJSONObject(0).has("responseObject");
        job_config = payload2.getJobConfig();
        assert job_config.getString("test_job_config").equals("test");
    }

}