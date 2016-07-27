package lemongrenade.core.models;

import org.json.JSONObject;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;

import java.text.SimpleDateFormat;
import java.util.UUID;

/**
 * Tasks are always stored in database until a job is "DELETED" or "PURGED"
 */
@Entity(value = "tasks", noClassnameStored = true)
public class LGTask  {

    public final static int TASK_STATUS_PROCESSING = 1;    // Task is QUEUED or an adapter is chewing on it
    public final static int TASK_STATUS_COMPLETE   = 2;    // Adapter successfully completed this task
    public final static int TASK_STATUS_FAILED     = 3;    // Adapter failed to process this task

    @Id
    private String taskId;
    @Indexed
    private String jobId;
    private String adapterId;
    private String adapterName;
    private long startTime;
    private long endTime;
    private int status;
    private int nodeCount;

    public LGTask() {
    }

    public LGTask( LGJob job, String adapterId, String adapterName, int nodeCount) {
        this.jobId = job.getJobId();
        this.adapterId = adapterId;
        this.adapterName = adapterName;
        this.taskId = UUID.randomUUID().toString();  // Assign an unique task Id
        this.startTime = System.currentTimeMillis();
        this.status = TASK_STATUS_PROCESSING;
        this.nodeCount = nodeCount;
    }

    public String getTaskId() { return taskId;}
    public void setTaskId(String id) { taskId= id; }
    public String getJobId() { return this.jobId; }
    public void setJobId(String jobId) { this.jobId = jobId;}
    public String getAdapterId() { return adapterId;}
    public void setAdapterId(String adapterId) { this.adapterId = adapterId; }
    public String getAdapterName() { return adapterName;}
    public void setAdapterName(String adapterName) { this.adapterName = adapterName; }
    public Long getStartTime() { return startTime; }
    public Long getEndTime()   { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime;}
    public int  getStatus() { return status; }
    public void setStatus(int status) { this.status = status;}
    public int  getNodeCount() { return nodeCount; }
    public void setNodeCount(int nodeCount) { this.nodeCount = nodeCount; }

    /** */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[TASK ID:").append(getTaskId())
                .append(" AdapterName:").append(adapterName)
                .append(" AdapterId:").append(adapterId).append(" ")
                .append(" status:").append(getStatusString())
                .append(" StartTime:").append(startTime)
                .append(" EndTime:").append(endTime)
                .append(" Nodes: ").append(nodeCount)
                .append("] ");
        return sb.toString();
    }

    /** */
    public JSONObject toJson(){
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        return new JSONObject()
                .put("task_id", this.taskId)
                .put("job_id",this.jobId)
                .put("adapter_id",this.getAdapterId())
                .put("adapter_name",this.getAdapterName())
                .put("start_time",sdf.format(this.getStartTime()))
                .put("end_time",sdf.format(this.getEndTime()))
                .put("nodes",getNodeCount())
                .put("status",getStatusString());
    }

    /** */
    public String getStatusString() {
        switch (status) {
            case TASK_STATUS_COMPLETE:   return "complete";
            case TASK_STATUS_FAILED:     return "failed";
            case TASK_STATUS_PROCESSING: return "processing";
        }
        return "unknown("+status+")";
    }

    /** */
    public static void main(String[] args)  {
        LGJob  lj = new LGJob(UUID.randomUUID().toString());
        JSONObject tuple = new JSONObject();
        LGTask lg = new LGTask(lj, "adapter", "adaptername",10);
        System.out.println(lg.toString());
    }

}
