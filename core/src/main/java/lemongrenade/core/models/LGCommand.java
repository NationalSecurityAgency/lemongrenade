package lemongrenade.core.models;

import org.apache.storm.shade.org.apache.commons.lang.SerializationUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;

public class LGCommand implements Serializable {
    public final static int COMMAND_TYPE_NEW             = 1;
    public final static int COMMAND_TYPE_ADD             = 2;
    public final static int COMMAND_TYPE_STOP            = 3;
    public final static int COMMAND_TYPE_DELETE          = 4;
    public final static int COMMAND_TYPE_GET_STATUS      = 5;
    public final static int COMMAND_TYPE_CHANGE_PRIORITY = 6;
    public final static int COMMAND_TYPE_POST_ACTION     = 7;
    public final static int COMMAND_TYPE_RESET           = 8;
    public final static int COMMAND_TYPE_NOOP            = 10;
    public final static int COMMAND_TYPE_RETRY_FAILED_TASKS = 11;
    private int cmd = COMMAND_TYPE_NOOP;
    private String jobId;
    private int ttl;
    private int priority;
    private ArrayList<String> adapterList;
    private LGPayload seedPayload;

    /**
     * @param cmd int for cmd
     * @param jobId String for job ID
     * @param ttl int for time to live
     * @param priority int for priority
     * @param adapterList ArrayList of Strings for adapter list
     * @param seedPayload LGPayload
     */
    public LGCommand(int cmd, String jobId, int ttl, int priority, ArrayList<String> adapterList, LGPayload seedPayload) {
        this.cmd = cmd;
        this.jobId = jobId;
        this.ttl   = ttl;
        this.priority = priority;
        this.adapterList = adapterList;
        this.seedPayload = seedPayload;
    }

    public int getCmd()      { return this.cmd; }
    public String getJobId() { return this.jobId;}
    public int getPriority() { return this.priority; }
    public void setPriority(int priority) { this.priority = priority; }
    public ArrayList<String> getAdapterList() { return this.adapterList; }
    public LGPayload getSeedPayload() { return this.seedPayload; }

    public static LGCommand deserialize(byte[] serialized) {
        return (LGCommand) SerializationUtils.deserialize(serialized);
    }

    /**
     * @return String for command type
     */
    public String getCmdString() {
        switch(this.cmd) {
            case COMMAND_TYPE_NEW:             return "NEW";
            case COMMAND_TYPE_ADD:             return "ADD";
            case COMMAND_TYPE_STOP:            return "STOP";
            case COMMAND_TYPE_DELETE:          return "DELETE";
            case COMMAND_TYPE_GET_STATUS:      return "GET_STATUS";
            case COMMAND_TYPE_CHANGE_PRIORITY: return "CHANGE_PRIORITY";
            case COMMAND_TYPE_POST_ACTION:     return "POST_ACTION";
            case COMMAND_TYPE_NOOP:            return "NOOP";
            case COMMAND_TYPE_RESET:           return "RESET";
            case COMMAND_TYPE_RETRY_FAILED_TASKS: return "RETRY";
        }
        return "UNKNOWN";
    }

    /**
     * @return JSONObject versino of LGCommand object
     */
    public JSONObject toJson() {
        JSONObject jo = new JSONObject();
        jo.append("cmd",cmd);
        jo.append("job_id",jobId);
        jo.append("ttl", ttl);
        jo.append("priority",priority);
        JSONArray adapterListJson = new JSONArray(adapterList);
        jo.append("adapterList",adapterListJson);
        jo.append("payload",seedPayload.toJsonString());
        return jo;
    }

    /**
     * @return String in JSONObject format
     */
    public String toJsonString() {
        return this.toJson().toString();
    }

    /** */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Cmd ").append(this.getCmdString());
        sb.append(" id :").append(this.jobId);
        sb.append(" ttl:").append(this.ttl);
        sb.append(" priority:").append(this.priority);
        sb.append(" payload:").append(this.seedPayload.toString());
        sb.append(" adapters:").append(adapterList.toString());
        return sb.toString();
    }

    /**
     * @return Returns this class as byte[]
     */
    public byte[] toByteArray() {
        return SerializationUtils.serialize(this);
    }

}
