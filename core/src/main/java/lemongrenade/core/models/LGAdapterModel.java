package lemongrenade.core.models;

import org.json.JSONObject;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;

import java.beans.Transient;
import java.util.HashMap;

@Entity("adapters")
public class LGAdapterModel {
    public final static int  STATUS_NEW    = 1;   // Adapter just registered
    public final static int  STATUS_ONLINE = 2;   // Up and running everything normal
    public final static int  STATUS_DEGRADED = 3; // Up but having problems or running slowly
    public final static int  STATUS_OFFLINE  = 4; // Heartbeat fell below threshold and considered to be offline

    @Id
    private String id;          // Requrired UUID format
    @Indexed
    private String name;        // Name (lowercase)
    private String graphQuery;  // Required
    private int    graphDepth;  // Required
    private int    status;
    private long   lastHeartBeat;
    private int    taskCount;
    private long   startTime;
    private int    maxNodesPerTask;   // Overrides the global maxNodesPerTask value
    private int    deadLetterCount;
    private long   lastDeadLetterTime;
    private String currentTaskId;
    private String extraParams;

    // String of JSON (otherwise mongo complains storing it) - Used by INTERNAL processing
    @Embedded
    private HashMap<String,String> requiredKeys;

    public LGAdapterModel() {}

    /**
     * @param id String for ID
     * @param name String for name
     * @param graphQuery String for graph query
     * @param graphDepth int of graph depth
     * @param maxNodesPerTask int of max nodes per task
     * @param extraParams JSONObject of extra params
     */
    public LGAdapterModel(String id, String name, String graphQuery, int graphDepth, int maxNodesPerTask, JSONObject extraParams) {
        this.id = id;
        this.name = name;
        this.status = STATUS_NEW;
        this.graphQuery = graphQuery;
        this.graphDepth = graphDepth;
        this.maxNodesPerTask = maxNodesPerTask;
        this.startTime = System.currentTimeMillis();
        this.lastHeartBeat = System.currentTimeMillis();
        this.taskCount = 0;
        this.deadLetterCount = 0;
        this.lastDeadLetterTime = 0;
        this.requiredKeys = new HashMap<String,String>();
        this.currentTaskId = "";
        this.extraParams = extraParams.toString();
    }


    //Getter/Setters
    public String getId() { return id;}
    public String getName() { return name;}
    public String getUniqueName() { return name+"-"+id; }
    public String getGraphQuery() { return graphQuery;  }
    public int    getGraphDepth() { return graphDepth;  }
    public int    getMaxNodesPerTask() { return maxNodesPerTask; }

    public long   getStartTime()  { return startTime;   }
    public long   getLastHeartBeat() { return lastHeartBeat; }
    public void   setLastheartBeat(long lastHeartBeat) { this.lastHeartBeat = lastHeartBeat; }
    public int    getTaskCount()  { return taskCount; }
    public void   setTaskCount(int taskCount) { this.taskCount = taskCount; }
    public int    getDeadLetterCount() { return deadLetterCount; }
    public void   setDeadLetterCount(int deadLetterCount) { this.deadLetterCount = deadLetterCount;}
    public long   getLastDeadLetterTime() { return lastDeadLetterTime; }
    public void   setLastDeadLetterTime(long lastDeadLetterTime){ this.lastDeadLetterTime = lastDeadLetterTime;}
    public HashMap<String,String> getRequiredKeys()    { return requiredKeys; }
    public void   setRequiredKeys(HashMap<String,String> requiredKeys){ this.requiredKeys = requiredKeys;}
    public String getCurrentTaskId()   { return currentTaskId; }
    public void   setCurrentTaskId(String currentTaskId) { this.currentTaskId = currentTaskId; }
    public int    getStatus() { return this.status; }
    public void   setStatus(int status) { this.status = status; }
    public String getStatusString() { return getStatusString(this.getStatus());}
    public String getAuxInfo() { return this.extraParams;}

    /**
     * @return Returns string version of LGAdapterModel
     */
    @Transient
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[Adapter ").append(getUniqueName())
                .append(" GraphQuery:").append(getGraphQuery())
                .append(" GraphDepth:").append(getGraphDepth())
                .append(" LastHeartBeat:").append(getLastHeartBeat())
                .append(" StartTime:").append(getStartTime())
                .append(" TaskCount:").append(getTaskCount())
                .append(" Status: ").append(getStatusString(getStatus()))
                .append(" DeadLetterCount:").append(getDeadLetterCount())
                .append(" MaxNodesPerTask:").append(getMaxNodesPerTask())
                .append("] ");
        return sb.toString();
    }

    /**
     * @return Returns JSONObject version of LGAdapterModel data
     */
    @Transient
    public JSONObject toJson() {
        long diffTime = (System.currentTimeMillis() - this.getLastHeartBeat())/ 1000;
        long uptime = (System.currentTimeMillis() - this.getStartTime())/1000;
        JSONObject data = new JSONObject()
                .put("id", getId())
                .put("name",getName())
                .put("type",getName())
                .put("unique_name",getUniqueName())
                .put("graph_query", getGraphQuery())
                .put("graph_depth", getGraphDepth())
                .put("last_heartbeat_raw", getLastHeartBeat())
                .put("last_hb",diffTime)
                .put("start_time", getStartTime())
                .put("uptime",uptime)
                .put("task_count", getTaskCount())
                .put("deadletter_count", getDeadLetterCount())
                .put("status", getStatusString(getStatus()))
                .put("max_nodes_per_task", getMaxNodesPerTask())
                .put("requiredkeys",getRequiredKeys())
            ;
        try {
            data.put("aux_info", new JSONObject(getAuxInfo()));
        }
        catch(Exception e) {
            data.put("aux_info", "{}");
        }
        return data;
    }


    /**
     * See notes at top for description of status types
     *
     * @param status int of status
     * @return The string value of the input status
     */
    @Transient
    public String getStatusString(int status) {
        switch(status) {
            case STATUS_NEW:
                return "NEW";
            case STATUS_ONLINE:
                return "ONLINE";
            case STATUS_DEGRADED:
                return "DEGRADED";
            case STATUS_OFFLINE:
                return "OFFLINE";
        }
        return "unknown";
    }

    public static void main(String[] args)  {
        LGAdapterModel lgAdapter = new LGAdapterModel("0000-0000-0000-0000-00001", "test", "query1", 5,0, new JSONObject());
        System.out.println(lgAdapter.toString());
    }
}
