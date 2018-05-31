package lemongrenade.core.models;

import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.util.TimeUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mongodb.morphia.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Transient;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Note: job_config is stored as a string so mongo can store it. If we stored as JSONObject, we were running
 * into problems where .'s or $'s in the key names would cause exceptions. Also, we expanded the details
 * in the job_config, especially the adapter:
 *     job_config : {
 *            adapter : {
 *                adapter1 : { depth:1, something:2, more:{ ... }}
 *            }
 *     }
 *     And Mongo/Morphia really didn't like this because it tried to map it.
 *
 *  So, if there's anything important in the job_config that it wants to access frequently, we parse it out
 *  and store it independently in it's own field. Otherwise, job_config is stored as string.
 *
 * Job States
 * ----------
 * RESET
 *    means the "graph" data has been removed from the database, but all the job meta data (seed, params,
 *    adapters, history, and so on) is maintained in the core database (Mongo at the moment) The idea is that a
 *    job can be 'reran' in the future. (just as it were resubmitted)
 *
 * ERROR:
 *    Something tragically went wrong and the job processing stopped and status was set to ERROR. You can
 *    interpret this as the job is "FINISHED" but unsuccessfully for whatever reason.
 *
 * NEW: Job has been submitted by the APi or command line tool but hasn't been picked up by the coordinator yet
 *
 * QUEUED:
 *    Created by the coordinatorCommandQueue but hasn't entered the processing queue yet
 *
 * PROCESSING:
 *    The coordinator is actively processing the job. It will remain in processing state until there are no
 *    more matching adapter matches and no active adapter tasks for this job.
 *
 * FINISHED:
 *    All adapters are done with the job and there are no more matching data to activate new adapters
 *
 * FINISHED_WITH_ERRORS:
 *    All adapters are done with the job and there are no more matching data to activate new adapters BUT
 *    there were one or more tasks that FAILED
 *
 * STOPPED:
 *    A stop command was received by the API on an active processing job. Job is set to STOPPING until all
 *    adapters are done chewing on the tasks that were running when the STOP command was first received. Once they are
 *    all stopped, the job itself is considered STOPPED.
 *
 * EXPIRED : TBD
 *
 */
@Entity(value = "jobs", noClassnameStored = true)
public class LGJob {
    private static final Logger log = LoggerFactory.getLogger(LGJob.class);

    public static final int JOB_ID_LENGTH = 36;
    public final static int STATUS_ERROR      = -1;
    public final static int STATUS_UNKNOWN    = 0;
    public final static int STATUS_NEW        = 1;
    public final static int STATUS_PROCESSING = 2;
    public final static int STATUS_FINISHED   = 4;
    public final static int STATUS_QUEUED     = 5;
    public final static int STATUS_STOPPED    = 7;
    public final static int STATUS_EXPIRED    = 8;
    public final static int STATUS_RESET      = 9;
    public final static int STATUS_FINISHED_WITH_ERRORS = 10;
    public final static int DEFAULT_EXPIRE_DAYS = 7;
    public final static int DEFAULT_RESET_DAYS = 1;
    public final static long SECOND = 1000;//1000 ms
    public final static long MINUTE = SECOND*60;
    public final static long HOUR = MINUTE*60;
    public final static long DAY = HOUR*24;
    private final static String DOT_CODE = "%2E";

    @Id
    private String jobId;
    @Reference
    private Map<String, LGTask> taskMap;
    @Embedded
    private List<String> approvedAdapterNames;  // NAME not ID of adapter allowed to run
    @Embedded
    private List<LGJobHistory> jobHistory;
    @Embedded
    private List<LGJobError> jobErrors;

    private HashMap<String, List<LGJobError>> taskErrorsMap; //taskId as key for a list of LGJObErrors for that task

    protected Date createDate;
    private long startTime;
    private long endTime;
    private long totalRunningTimeSeconds;  // in case goes from finished->queued
    private int status;
    private long lastTaskTime;
    private int coordinatorId;
    private String jobConfig;
    private String reason;
    private long expireDate;
    private long resetDate;
    private int graphActivity;  // MAX maxGraphId that we know about for job, which gives us a relative graph size
    private String lg_type;

    public LGJob() {
        long now = System.currentTimeMillis();
        this.startTime = now;
        this.resetDate = now+DEFAULT_RESET_DAYS*DAY;
        this.expireDate = now+DEFAULT_EXPIRE_DAYS*DAY;
        this.taskMap = new HashMap<String, LGTask>();
        this.jobHistory = new ArrayList<LGJobHistory>();
        this.jobErrors  = new ArrayList<LGJobError>();
        this.taskErrorsMap  = new HashMap<String, List<LGJobError>>();
        this.createDate = new Date();
        this.graphActivity = 0;
        this.totalRunningTimeSeconds = 0;
        this.taskMap = new ConcurrentHashMap<String, LGTask>();
        this.status  = STATUS_NEW;
        this.approvedAdapterNames = new ArrayList<String>();
        this.jobConfig  = new JSONObject().toString();
        this.reason     = "";
        this.lg_type = "graph";
    }

    public LGJob(String jobId) {
        this();
        this.jobId = jobId;
    }

    public LGJob(String jobId, ArrayList<String> approvedAdapterNameList, JSONObject jobConfig) {
        this(jobId);
        if (jobConfig == null) {
            jobConfig = new JSONObject();
        }

        try {
            if (jobConfig.has("reset_date")) {
                String iso = jobConfig.getString("reset_date");
                Date date = TimeUtils.parseIso8601(iso);
                this.resetDate = TimeUtils.getEpoch(date);
            }
            if (jobConfig.has("expire_date")) {
                String iso = jobConfig.getString("expire_date");
                Date date = TimeUtils.parseIso8601(iso);
                this.expireDate = TimeUtils.getEpoch(date);
            }
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }

        this.jobConfig = jobConfig.toString();
        for (String a : approvedAdapterNameList) {
            this.addApprovedAdapter(a.toLowerCase());
        }
    }

    //sets the start time for this job anew. Only use for post actions in order to calculate the new duration.
    public void setStartTime() {
        this.startTime = System.currentTimeMillis();
    }

    //sets the start time for this job anew. Only use for post actions in order to calculate the new duration.
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    @PrePersist
    public void prePersist() {
        createDate = (createDate == null) ? new Date() : createDate;
        reason = (reason == null) ? "" : reason;
    }

    // Setter/Getters for persistence
    public String getJobId() {return this.jobId;}
    public void setJobId(String jobId) {this.jobId = jobId;}
    public int getStatus() {return this.status;}
    public int getCoordinatorId() { return this.coordinatorId; }
    public void setCoordinatorId(int coordinatorId) { this.coordinatorId = coordinatorId; }
    public Date getCreateDate() { return this.createDate;}
    public void setCreateDate(Date createDate ) { this.createDate = createDate;}
    public long getExpireDate() { return this.expireDate;}
    public void setExpireDate(long expireDate ) { this.expireDate = expireDate;}
    public long getResetDate() { return this.resetDate;}
    public void setResetDate(long resetDate ) { this.resetDate = resetDate;}
    public String getReason() { return this.reason; }
    public void setReason(String reason) { this.reason = reason; }
    public int getGraphActivity() { return this.graphActivity; }
    public void setGraphActivity(int graphActivity) { this.graphActivity = graphActivity; }
    public String getJobConfig() { return jobConfig.toString(); }

    public JSONObject getJobConfigAsJSON() {
        if (this.jobConfig == null) { return new JSONObject(); }
        return new JSONObject(this.jobConfig);
    }

    public void setJobConfig(String jobConfig) {
        this.jobConfig = jobConfig;
    }

    /**
     * Approved lemongrenade.adapters are lemongrenade.adapters that this job is allowed to run (we give permission
     * to run at create time.) If this list is empty, it's not allowed to run on any lemongrenade.adapters.
     * remember: All approvedAdapter functions use adapterName not adapterId
     * @return List of Strings for approved adapters
     */
    public List<String> getApprovedAdapterList() {
        if (approvedAdapterNames == null) {
            return new ArrayList<String>();
        }
        return approvedAdapterNames;
    }

    public Set<String> getApprovedAdapterSet() {
        HashSet<String> set = new HashSet();
        Iterator iterator = approvedAdapterNames.iterator();
        while(iterator.hasNext()) {
            String adapter = (String) iterator.next();
            set.add(adapter);
        }
        return set;
    }

    public void setApprovedAdapterList(final List<String> approvedAdapterNames) {
        this.approvedAdapterNames = approvedAdapterNames;
    }

    public void addApprovedAdapter(String adapterName) {
        if (approvedAdapterNames.contains(adapterName.toLowerCase())) {
            return;
        }
        approvedAdapterNames.add(adapterName.toLowerCase());
    }

    public boolean isAdapterApproved(String adapterName) {
        if (approvedAdapterNames.contains(adapterName.toLowerCase())) {
            return true;
        }
        return false;
    }

    public void removeApprovedAdapter(String adapterName) {
        if (approvedAdapterNames.contains(adapterName.toLowerCase())) {
            return;
        }
        approvedAdapterNames.remove(adapterName.toLowerCase());
    }

    /**
     * @param status int for status
     * @return true if status changed
     * @throws InvalidJobStateChangeException thrown for invalid state change
     */
    @Transient
    public boolean setStatus(int status) throws InvalidJobStateChangeException {
        // If we are already in state, just return
        if (status == this.getStatus()) {
            return false;
        }
        if (!this.checkStatus(status)) {
            throw new InvalidJobStateChangeException("Invalid State Change from "
                                                     + this.getStatusString(this.getStatus())
                                                     + " to " + this.getStatusString(status));
        }
        this.status = status;
        return true;
    }

    public long getLastTaskTime() {return lastTaskTime;}

    public void setLastTaskTime(long lastTaskTime) {this.lastTaskTime = lastTaskTime;}

    public long getStartTime() {return startTime;}

    public long getEndTime() {return endTime;}

    public void setEndTime(long endTime) {this.endTime = endTime;}

    public long getTotalRunningTimeSeconds() {return totalRunningTimeSeconds;}

    public void setTotalRunningTimeSeconds(long totalRunningTimeSeconds) {
        this.totalRunningTimeSeconds = totalRunningTimeSeconds;
    }

    public List<LGJobHistory> getJobHistory() {return jobHistory;}

    public void setJobHistory(List<LGJobHistory> jobHistory) {
        this.jobHistory = jobHistory;
    }

    public List<LGJobError> getJobErrors() {
        return jobErrors;
    }

    public void setJobErrors(List<LGJobError> jobErrors) { this.jobErrors = jobErrors; }

    /**
     * @return long of current running time in seconds
     */
    @Transient
    public long calculateCurrentRunningTimeSeconds() {
        long totalRunningSeconds = totalRunningTimeSeconds + ((System.currentTimeMillis() - this.getStartTime()) / 1000);
        return totalRunningSeconds;
    }

    /**
     * @return long for end time
     */
    @Transient
    public long calculateEndTime() {
        long _endTime = System.currentTimeMillis();
        this.setEndTime(_endTime);
        long totalRunningSeconds = this.getTotalRunningTimeSeconds() + calculateDuration(startTime, endTime);
        this.setTotalRunningTimeSeconds(totalRunningSeconds);
        return totalRunningSeconds;
    }

    public static long calculateDuration(long startTime, long endTime) {
        long duration = ((endTime - startTime) / 1000);
        return duration;
    }

    public Map<String,LGTask> getTasks() {return taskMap;}

    public Map<String,LGTask> getTaskMap() {
        return taskMap;
    }

    public void setTaskMap( HashMap<String,LGTask> taskMap) {
        this.taskMap = taskMap;
    }

    public void addTask(LGTask task) {
        lastTaskTime = System.currentTimeMillis();
        taskMap.put(task.getTaskId(), task);
    }

    /**
     * Get taskCount() is the number of tasks that this job has regardless of the task status
     * @return int for task map size
     */
    @Transient
    public int getTaskCount() {
        if (taskMap == null) {
            return 0;
        }
        return taskMap.size();
    }

    /**
     * GetErrorCount() is the number of errors that this job has stored in errors
     * @return int for job error count
     */
    @Transient
    public int getErrorCount() {
        if ( jobErrors == null) {
            return 0;
        }
        return jobErrors.size();
    }

    /** Returns the number of tasks that are active, which is "status:PROCESSING".
     * Don't confuse this with getTaskCount()
     * @return int for active task count
     */
    @Transient
    public int getActiveTaskCount() {
        if (taskMap == null) {
            return 0;
        }
        int activeCount = 0;
        for(Map.Entry<String,LGTask> entry : taskMap.entrySet()) {
            LGTask t = entry.getValue();
            if (t.getStatus() == LGTask.TASK_STATUS_PROCESSING) {
                activeCount++;
            }
        }
        return activeCount;
    }

    public static int getActiveTaskCount(JSONObject tasks) {
        if(tasks == null) {
            return 0;
        }
        int activeCount = 0;
        Iterator taskIterator = tasks.keys();
        while(taskIterator.hasNext()) {
            String key = taskIterator.next().toString();
            JSONObject task = tasks.getJSONObject(key);
            int status = task.getInt("status");
            if (status == LGTask.TASK_STATUS_PROCESSING) {
                activeCount++;
            }
        }
        return activeCount;
    }

    public void updateTaskStatus(String taskId, int status) {
        if(taskMap == null) {
            return;
        }
        if(taskMap.keySet().contains(taskId)) { //the given taskId is present
            LGTask task = taskMap.get(taskId);
            task.setStatus(status);
        }
    }

    public void updateTask(LGTask task) {
        if(taskMap == null || task == null) {
            return;
        }
        String taskId = task.getTaskId();
        if(taskMap.keySet().contains(taskId)) { //the given taskId is present
            taskMap.put(taskId, task);
        }
    }

    /** Returns the number of tasks that are active, which is "status:FAILED".
     * Don't confuse this with getTaskCount() or getActiveTaskCount()
     * @return int of failed task count
     */
    @Transient
    public int getFailedTaskCount() {
        if (taskMap == null) {
            return 0;
        }
        int failed = 0;
        for(Map.Entry<String,LGTask> entry : taskMap.entrySet()) {
            LGTask t = entry.getValue();
            if (t.getStatus() == LGTask.TASK_STATUS_FAILED) {
                failed++;
            }
        }
        return failed;
    }

    @Transient
    public void addHistory(LGJobHistory historyLine) {
        jobHistory.add(historyLine);
    }

    @Transient
    public void addError(LGJobError error) {
        jobErrors.add(error);
    }

    @Transient
    public void removeErrorsByTaskId(String taskId) {
        Iterator<LGJobError> ij = jobErrors.iterator();
        while(ij.hasNext()) {
            LGJobError je =  ij.next();
            String errorTaskId = je.getTaskId();
            if (errorTaskId != null && errorTaskId.equals(taskId)) {
                ij.remove();
            }
        }
    }

    @Transient
    public void delTask(String taskId) {
        if (taskMap.containsKey(taskId)) {
            taskMap.remove(taskId);
        }
    }

    /**
     *
     * @return JSONArray of LGTask items as JSON
     */
    @Transient
    public JSONArray getTaskList() {
        JSONArray gt = new JSONArray();
        taskMap.forEach((k,v) -> {
            LGTask t = taskMap.get(k);
            gt.put(t.toJson());
        });
        return gt;
    }

    public LGTask getTask(String taskId) {
        return taskMap.get(taskId);
    }

    @Transient
    public boolean checkStatus(int newStatus) {
        return checkStatus(this.status, newStatus);
    }

    /**
     * Changes the job state. States follow state machine rules.
     * @param oldStatus int value of old status
     * @param newStatus int value for new status
     * @return boolean if success otherwise throws InvalidStateChangeException
     */
    @Transient
    public static boolean checkStatus(int oldStatus, int newStatus) {
        switch (oldStatus) {
            case STATUS_UNKNOWN: //treat the same as STATUS_NEW
                if ((newStatus != STATUS_QUEUED)
                        && (newStatus != STATUS_PROCESSING)
                        && (newStatus != STATUS_STOPPED)
                        && (newStatus != STATUS_EXPIRED)
                        && (newStatus != STATUS_FINISHED) //possible for FINISHED case from NEW if no adapters trigger
                        && (newStatus != STATUS_FINISHED_WITH_ERRORS)//occurs from NEW when no adapter list is provided
                        && (newStatus != STATUS_ERROR)) { //possible for ERROR case from NEW if no adapter listed provided
                    return false;
                }
                break;
            case STATUS_NEW:
                if ((newStatus != STATUS_QUEUED)
                        && (newStatus != STATUS_PROCESSING)
                        && (newStatus != STATUS_STOPPED)
                        && (newStatus != STATUS_EXPIRED)
                        && (newStatus != STATUS_FINISHED) //possible for FINISHED case from NEW if no adapters trigger
                        && (newStatus != STATUS_FINISHED_WITH_ERRORS)//occurs from NEW when no adapter list is provided
                        && (newStatus != STATUS_ERROR)) { //possible for ERROR case from NEW if no adapter listed provided
                    return false;
                }
                break;
            case STATUS_QUEUED:
                if ((newStatus != STATUS_NEW) && (newStatus != STATUS_QUEUED) && (newStatus != STATUS_ERROR)) {
                    return false;
                }
                break;
            case STATUS_RESET:
                if ((newStatus != STATUS_PROCESSING)
                        && (newStatus != STATUS_ERROR)) {
                    return false;
                }
                break;
            case STATUS_PROCESSING:
                if ((newStatus != STATUS_FINISHED)
                        && (newStatus != STATUS_FINISHED_WITH_ERRORS)
                        && (newStatus != STATUS_PROCESSING)
                        && (newStatus != STATUS_STOPPED)
                        && (newStatus != STATUS_EXPIRED)
                        && (newStatus != STATUS_ERROR)) {
                    return false;
                }
                break;
            case STATUS_FINISHED:
                if ((newStatus != STATUS_QUEUED)
                        && (newStatus != STATUS_RESET)
                        && (newStatus != STATUS_ERROR)
                        && (newStatus != STATUS_PROCESSING)
                        && (newStatus != STATUS_FINISHED_WITH_ERRORS)
                        && (newStatus != STATUS_FINISHED)
                    )
                {
                    return false;
                }
                break;
            case STATUS_FINISHED_WITH_ERRORS:
                if ((newStatus != STATUS_QUEUED)
                        && (newStatus != STATUS_RESET)
                        && (newStatus != STATUS_ERROR)
                        && (newStatus != STATUS_PROCESSING)
                        && (newStatus != STATUS_FINISHED_WITH_ERRORS)
                        && (newStatus != STATUS_FINISHED)
                        )
                {
                    return false;
                }
                break;
            case STATUS_STOPPED:
                if ((newStatus != STATUS_QUEUED)
                        && (newStatus != STATUS_RESET)
                        ) {
                    return false;
                }
                break;
            case STATUS_EXPIRED:
                if ((newStatus != STATUS_FINISHED)
                        && (newStatus != STATUS_FINISHED_WITH_ERRORS)
                        && (newStatus != STATUS_RESET)
                        && (newStatus != STATUS_STOPPED)) {
                    return false;
                }
                break;
            case STATUS_ERROR:
                if (newStatus != STATUS_QUEUED
                        && (newStatus != STATUS_FINISHED_WITH_ERRORS))
                {
                    return false;
                }
                break;
            default:
                return false;
        }
        return true;
    }

    /**
     * Simple method to covert status to a human readable string
     *
     * @return String
     */
    @Transient
    public static String getStatusString(int _status) {
        switch (_status) {
            case STATUS_ERROR:
                return "ERROR";
            case STATUS_NEW:
                return "NEW";
            case STATUS_QUEUED:
                return "QUEUED";
            case STATUS_PROCESSING:
                return "PROCESSING";
            case STATUS_FINISHED:
                return "FINISHED";
            case STATUS_FINISHED_WITH_ERRORS:
                return "FINISHED_WITH_ERRORS";
            case STATUS_STOPPED:
                return "STOPPED";
            case STATUS_EXPIRED:
                return "EXPIRED";
            case STATUS_RESET:
                return "RESET";
            default:
                return "UNKNOWN :"+_status;
        }
    }

    /**
     * Returns the Status int value for a string. Returns 0 if it doesn't know the given string
     * @param _status status value to lookup
     * @return int the int value that's stored in db
     */
    @Transient
    public static int getStatusByString(String _status) {
        if (_status.equalsIgnoreCase("ERROR"))           { return STATUS_ERROR;  }
        else if (_status.equalsIgnoreCase("NEW"))        { return STATUS_NEW;  }
        else if (_status.equalsIgnoreCase("QUEUED"))     { return STATUS_QUEUED;  }
        else if (_status.equalsIgnoreCase("PROCESSING")) { return STATUS_PROCESSING;  }
        else if (_status.equalsIgnoreCase("FINISHED"))   { return STATUS_FINISHED;  }
        else if (_status.equalsIgnoreCase("FINISHED_WITH_ERRORS")) { return   STATUS_FINISHED_WITH_ERRORS;  }
        else if (_status.equalsIgnoreCase("STOPPED"))    { return STATUS_STOPPED;  }
        else if (_status.equalsIgnoreCase("EXPIRED"))    { return STATUS_EXPIRED;  }
        else if (_status.equalsIgnoreCase("RESET"))      { return   STATUS_RESET;  }
        else { return 0;  }
    }

    /** Job Config accessors */
    @Transient
    public int getDepth() {
        JSONObject jc = this.getJobConfigAsJSON();
        if (jc == null) return -1;
        if (jc.has("depth")) {
            return jc.getInt("depth");
        }
        return -1;
    }

    @Transient
    public int getTTL() {
        JSONObject jc = this.getJobConfigAsJSON();
        if (jc == null) return -1;
        if (jc.has("ttl")) {
            return jc.getInt("ttl");
        }
        return -1;
    }

    @Transient
    public String getDescription() {
        JSONObject jc = this.getJobConfigAsJSON();
        if (jc == null) return "";
        if (jc.has("description")) {
            return jc.getString("description");
        }
        return "";
    }

    /**
     * @return String
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("job_id:").append(jobId);
        sb.append(" status:").append(this.getStatusString(this.getStatus()));
        sb.append(" taskcount:").append(this.getTaskCount());
        sb.append(" starttime:").append(this.getStartTime());
        sb.append(" lastTaskTime:").append(this.getLastTaskTime());
        sb.append(" graphActivity:").append(this.getGraphActivity());
        sb.append(" coordinatorid:").append(this.getCoordinatorId());
        if (this.approvedAdapterNames != null) {
            sb.append(" approvedadapterlist:").append(this.approvedAdapterNames.toString());
        }
        sb.append(" config:").append(this.getJobConfig());
        sb.append(" history:").append(this.jobHistory.toString());
        sb.append(" tasklist:").append(this.getTasks().toString());
        sb.append("create_date:").append(this.getCreateDate().toString());
        sb.append("expire_date:").append(this.getExpireDate());
        sb.append("reset_date:").append(this.getResetDate());
        sb.append("reason:").append(this.getReason());
        return sb.toString();
    }

    /** */
    public JSONObject toJson() {
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        JSONObject jo = new JSONObject(1);
        jo.put("job_id", jobId);
        jo.put("status", this.getStatusString(this.getStatus()));
        jo.put("starttime", sdf.format(this.getStartTime()));
        jo.put("endtime", sdf.format(this.getEndTime()));
        jo.put("runtime", this.getTotalRunningTimeSeconds());
        JSONArray adapters = new JSONArray(this.approvedAdapterNames);
        jo.put("approvedadapters", adapters);
        jo.put("job_config", this.getJobConfigAsJSON());
        jo.put("task_count", this.getTaskCount());
        jo.put("active_task_count", this.getActiveTaskCount());
        jo.put("error_count", this.getErrorCount());
        jo.put("create_date", sdf.format(this.getCreateDate()));
        jo.put("expire_date", sdf.format(this.getExpireDate()));
        jo.put("reset_date", sdf.format(this.getResetDate()));
        jo.put("graph_activity", this.getGraphActivity());
        jo.put("reason", this.getReason());
        return jo;
    }

    public HashMap<String, List<LGJobError>> getTaskErrorsMap() {
        return taskErrorsMap;
    }

    public void setTaskErrors(HashMap<String, List<LGJobError>> taskErrors) {
        this.taskErrorsMap = taskErrors;
    }

    //Gets the taskErrors for taskId, adds and returns an empty ArrayList if absent
    public List<LGJobError> getTaskErrors(String taskId) {
        if(!taskErrorsMap.containsKey(taskId)) {
            taskErrorsMap.put(taskId, new ArrayList<LGJobError>());
        }
        List<LGJobError> taskErrors = taskErrorsMap.get(taskId);
        return taskErrors;
    }

    //Adds a task Error
    public void addTaskError(String taskId, LGJobError error) {
        getTaskErrors(taskId).add(error);
    }

    //Forces the status and reason provided. Only use with force=true or after a checkStatus
    public void forceStatus(int status, String reason) {
        this.status = status;
        if(reason != null) {
            this.reason = reason;
        }
    }

    /** */
    public static void main(String[] args)  {
        ArrayList<String> alist = new ArrayList<String>();
        alist.add("adapter1");
        alist.add("adapter2");
        JSONObject jobConfig = new JSONObject();
        //jobConfig.put("bad.key","somevalue");

        jobConfig.put("goodkey","somevalue");
        LGJob  lj = new LGJob(UUID.randomUUID().toString(),alist,jobConfig);

        JobManager jobManager = new JobManager();

        jobManager.addJob(lj);
        LGJob j3 = jobManager.getJob(lj.getJobId());
        System.out.println(j3.toString());
        jobManager.close();
    }
}

