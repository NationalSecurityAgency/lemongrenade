package lemongrenade.core.models;

import org.json.JSONObject;

public class LGJobError {
    private long timestamp;
    private String jobId;
    private String adapter;
    private String adapterId;
    private String taskId;
    private String message;
    private static final String className = LGJobError.class.getName();

    public LGJobError() {}

    public LGJobError(String jobId, String taskId, String adapter, String adapterId, String message) {
        this.timestamp = System.currentTimeMillis();
        this.jobId = jobId;
        this.taskId = taskId;
        this.adapter = adapter;
        this.adapterId = adapterId;
        this.message = message;
    }

    public long getTimestamp() { return this.timestamp; }
    public String getJobId()   { return this.jobId;     }
    public String getTaskId()  { return this.taskId;    }
    public String getAdapter() { return this.adapter;   }
    public String getAdapterId() { return this.adapterId; }
    public String getMessage() { return this.message; }

    /** */
    public JSONObject toJson() {
    //SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
    //Date date = new Date(this.timestamp);
        JSONObject jo = new JSONObject(1)
        .put("className", className)
        .put("timestamp", this.timestamp)
        .put("jobId", this.jobId)
        .put("adapter", this.adapter)
        .put("adapterId", this.adapterId)
        .put("taskId", this.taskId)
        .put("message", this.message)
        ;
        return jo;
    }

}


