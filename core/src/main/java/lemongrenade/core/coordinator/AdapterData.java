package lemongrenade.core.coordinator;

/**
 * Data object used by the adapterManager to keep track of the lemongrenade.adapters
 * which provides basic loadbalancing and routing of tasks to lemongrenade.adapters.
 */
public class AdapterData {
    private int status;
    private String name;
    private String id;
    private int taskCount;
    private String requiredAttrs;
    private long lastHeartBeatTime;
    public int getStatus() {return status; }
    public void setStatus(int _status) {
        this.status = _status;
    }
    public String getName() { return this.name; }
    public void setName(String name) { this.name = name; }
    public String getId() { return this.id; }
    public void setId(String id) { this.id = id; }
    public int getTaskCount() { return taskCount;}
    public void setTaskCount(int taskCount) { this.taskCount = taskCount; }
    public long getLastHeartBeatTime() { return lastHeartBeatTime; }
    public void setLastHeartBeatTime(long lastHeartBeatTime) { this.lastHeartBeatTime = lastHeartBeatTime;}
    public String getRequiredAttrs() { return requiredAttrs; }

    public AdapterData(String _name, String _id, String _requiredAttrs, int _taskCount, int _status, long _lastHeartBeat) {
        taskCount = _taskCount;
        name = _name;
        id = _id;
        status = _status;
        requiredAttrs = _requiredAttrs;
        lastHeartBeatTime = _lastHeartBeat;
    }

}
