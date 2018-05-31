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
    public final static int TASK_STATUS_REPLAYED   = 4;    // When a task gets replayed after an ERROR/FAIL
    public final static int TASK_STATUS_DROPPED   = 5;    // When a task gets dropped due to a STOPPED job status

    @Id
    private String taskId;
    @Indexed
    private String jobId;
    private String parentTaskId;
    private String adapterId;
    private String adapterName;
    private long startTime;
    private long endTime;
    private int status;
    private int nodeCount;
    private int currentGraphId;
    private int maxGraphId;
    private int nodePageIndex;  // which page of nodes was sent in this task. See coordinator for info
    public LGTask() {
    }

    /**
     * Constructor
     *
     * @param job           - The job object that we are generating this task for
     * @param adapterId     - UUID of adapter
     * @param adapterName   - Adapter Name
     * @param nodeCount     - Number of nodes used in this task
     * @param parentTaskId   - The ID of the task that generated this task - leave 0 for seed or reset/replay/etc
     * @param currentGraphId - CurrentGrpahId being used to generated query data from LEMONGRAPH for this tasks
     *                         We use this for replay/retry command. This is the START index for LemonGraph Query
     * @param maxGraphId     - Value returned from LemonGraph - It's the STOP value for the LemonGraph Query
     * @param nodePageIndex  - The node page idx was sent to this task (nodes are split on conf.max_node_size_per_task)
     *
     */
    public LGTask( LGJob job, String adapterId, String adapterName, int nodeCount, String parentTaskId,
                   int currentGraphId, int maxGraphId, int nodePageIndex) {
        this.jobId = job.getJobId();
        this.adapterId = adapterId;
        this.adapterName = adapterName;
        this.taskId = UUID.randomUUID().toString();  // Assign an unique task Id
        this.startTime = System.currentTimeMillis();
        this.status = TASK_STATUS_PROCESSING;
        this.nodeCount = nodeCount;
        // Null is usually a seed OR a RETRY command
        if (parentTaskId == null) {
            parentTaskId = "";
        }
        this.parentTaskId = parentTaskId;
        this.currentGraphId = currentGraphId;
        this.maxGraphId     = maxGraphId;
        this.nodePageIndex       = nodePageIndex;
    }

    public String getTaskId() { return taskId;}
    public void setTaskId(String id) { taskId= id; }
    public String getParentTaskId() { return parentTaskId;}
    public void setParentTaskId(String id) { this.parentTaskId = id; }
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
    public int  getCurrenGraphId() { return this.currentGraphId; }
    public void setCurrentGraphId(int currentGraphId) { this.currentGraphId = currentGraphId; }
    public int  getMaxGraphId() { return this.maxGraphId; }
    public int  getNodePageIndex()   { return this.nodePageIndex; }

    /** */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[TASK ID:").append(getTaskId())
                .append(" Parent Task:").append(parentTaskId)
                .append(" AdapterName:").append(adapterName)
                .append(" AdapterId:").append(adapterId)
                .append(" Status:").append(getStatusString())
                .append(" StartTime:").append(startTime)
                .append(" EndTime:").append(endTime)
                .append(" Nodes:").append(nodeCount)
                .append(" CurrentGraphId:").append(currentGraphId)
                .append(" MaxGraphId:").append(maxGraphId)
                .append(" NodePageIndex:").append(nodePageIndex)
                .append("] ");
        return sb.toString();
    }

    /** */
    public JSONObject toJson(){
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        return new JSONObject()
                .put("task_id", this.taskId)
                .put("parent_task_id", this.parentTaskId)
                .put("job_id",this.jobId)
                .put("adapter_id",this.getAdapterId())
                .put("adapter_name",this.getAdapterName())
                .put("start_time",sdf.format(this.getStartTime()))
                .put("end_time",sdf.format(this.getEndTime()))
                .put("nodes",getNodeCount())
                .put("current_graph_id",getCurrenGraphId())
                .put("max_graph_id", getMaxGraphId())
                .put("node_page_index", getNodePageIndex())
                .put("status",getStatusString());
    }

    /** */
    public String getStatusString() {
        switch (status) {
            case TASK_STATUS_COMPLETE:   return "complete";
            case TASK_STATUS_FAILED:     return "failed";
            case TASK_STATUS_PROCESSING: return "processing";
            case TASK_STATUS_REPLAYED:   return "replayed";
            case TASK_STATUS_DROPPED:   return "dropped";
        }
        return "unknown("+status+")";
    }

    /** */
    public static void main(String[] args)  {
        LGJob  lj = new LGJob(UUID.randomUUID().toString());
        LGTask lg = new LGTask(lj, "adapter", "adaptername",10, "",0,0,0);
        System.out.println(lg.toString());
    }

}
