package lemongrenade.core.models;

import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LGJobError {
    private long timestamp;
    private String jobId;
    private String message;
    private String adapter;
    private String adapterId;
    private String errorMessage;
    private String taskId;

    public LGJobError() {}

    public LGJobError(String jobId, String taskId, String adapter, String adapterId, String errorMessage) {
        this.timestamp = System.currentTimeMillis();
        this.jobId = jobId;
        this.taskId = taskId;
        this.adapter = adapter;
        this.adapterId = adapterId;
        this.errorMessage = errorMessage;
    }

    public long getTimestamp() { return this.timestamp; }
    public String getJobId()   { return this.jobId;     }
    public String getTaskId()  { return this.taskId;    }
    public String getAdapter() { return this.adapter;   }
    public String getAdapterId() { return this.adapterId; }
    public String getErrorMessage() { return this.errorMessage; }

    /** */
    public JSONObject toJson() {
        JSONObject jo = new JSONObject(1);
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        Date date = new Date(this.timestamp);
        jo.put("timestamp", sdf.format(date));
        jo.put("jobid", this.jobId);
        jo.put("task_id", this.taskId);
        jo.put("adapter", this.adapter);
        jo.put("adapter_id", this.adapterId);
        jo.put("message", this.message);
        return jo;
    }

}


