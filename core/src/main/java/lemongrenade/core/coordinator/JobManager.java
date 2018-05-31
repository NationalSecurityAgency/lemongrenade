package lemongrenade.core.coordinator;

import com.mongodb.DBRef;
import lemongrenade.core.database.mongo.*;
import lemongrenade.core.models.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class JobManager {
    private static final Logger log = LoggerFactory.getLogger(JobManager.class);
    static private MorphiaService MS;
    static private LGJobDAOImpl JOB_DAO;
    static private LGTaskDAOImpl TASK_DAO;
    static private LGdbValueDAOImpl DBVALUE_DAO;
    static private MongoDBStore MONGO_DB_STORE;

    static {
        open();
    }

    public JobManager() {}// Constructor. Required, don't delete this.

    public static void close() {
        try {
            if(MONGO_DB_STORE != null) {
                MONGO_DB_STORE.close();
                MS.close();
                MS = null;
                MONGO_DB_STORE = null;
            }
        }
        catch(Exception e) {
            log.error("Error trying to close Mongo connections.");
            e.printStackTrace();
        }
    }

    public static void open() {
        if(MONGO_DB_STORE == null) {
            MONGO_DB_STORE = new MongoDBStore();
            MS = new MorphiaService();
            JOB_DAO  = new LGJobDAOImpl(LGJob.class, MS.getDatastore());
            TASK_DAO = new LGTaskDAOImpl(LGTask.class, MS.getDatastore());
            DBVALUE_DAO = new LGdbValueDAOImpl(LGdbValue.class, MS.getDatastore());
        }
    }

    /** Saves an entire job. Only use this when adding a new job. If overwriting a field, use a specific write.
     * This function is used to add NEW jobs not previously in the database.
     * DO NOT use this to update a subset of fields as it overwrites the entire job
     * @param job LGJob
     **/
    public static void addJob(LGJob job) {
        JOB_DAO.save(job);
    }

    public static LGJob getJob(String jobId) {
        open();
        return JOB_DAO.getByJobId(jobId);
    }

    public HashMap<String, LGJob> getJobs(JSONArray jobIds) {
        open();
        HashMap<String, LGJob> jobs = JOB_DAO.getByJobIds(jobIds);
        return jobs;
    }

    public boolean deleteJob(LGJob job) {
        if (job.getStatus() == LGJob.STATUS_PROCESSING) {
            log.error("Delete Failed: job is in processing state."+job.getJobId());
            return false;
        }
        open();
        JOB_DAO.delete(job);
        return true;
    }

    //Returns true if values were deleted. Returns false if no values were deleted (because none existed)
    public boolean deleteDBValues(String jobId) {
        try {
            open();
            DBVALUE_DAO.delete(jobId);
            return true;
        }
        catch(Exception e) {
            return false;
        }
    }

    public static boolean doesJobExist(String jobId) {
        try {
            LGJob j = getJob(jobId);
            if (j == null) {
                return false;
            }
            else {
                return true;
            }
        } catch(Exception e) {
            return false;
        }
    }

    public boolean doesJobExist(LGJob job) { return doesJobExist(job.getJobId()); }

    public boolean updateJobConfig(LGJob job, JSONObject config) {
        job.setJobConfig(config.toString());//updates the job config on LGJob object
        open();
        MONGO_DB_STORE.appendToJob(job.getJobId(), "jobConfig", config.toString());//updates the jobConfig in mongo
        return true;
    }

    //Updates the job status if the change is allowed. Returns "true" if new status is set
    public static boolean setStatus(LGJob job, int status) {return setStatus(job, status, null, false);}

    /**
     * Some states(RESET) we store a state value also
     *
     * @param job of job to modify in mongo
     * @param newStatus new status to change job to
     * @param reason string that describes why we performed this status change (for reset mostly)
     * @param force   cause the status and reason string to be updated no matter what
     * @return "true" if new status is set, otherwise false
     */
    public static boolean setStatus(LGJob job, int newStatus, String reason, boolean force) {
        int oldStatus = job.getStatus();
        String jobId = job.getJobId();
        boolean checkStatus = false;
        if (!force) {
            if(oldStatus == newStatus) {
                log.info("No status change required for job:"+jobId+" status:"+LGJob.getStatusString(oldStatus));
                return true;//returns true because ending status is "newStatus", as requested
            }
            checkStatus = LGJob.checkStatus(oldStatus, newStatus);
            if(checkStatus == false) {
                log.error("Invalid job state change from " + LGJob.getStatusString(oldStatus) + " to "
                        + LGJob.getStatusString(newStatus) + " for job:" + jobId);
                return false;//returns false because we failed to set the status to "newStatus"
            }
        }

        // Only update if status change is allowed or forced
        if ((force) || (checkStatus)) {
            open();
            if(reason != null) {
                MONGO_DB_STORE.appendToJob(jobId, "reason", reason);//updates the reason field in mongoDB
            }
            MONGO_DB_STORE.appendToJob(jobId, "status", newStatus);;//updates the status field in mongoDB
            job.forceStatus(newStatus, reason); //sets the LGJob object status and reason to the new values
            log.info("Job state changed from " + LGJob.getStatusString(oldStatus) + " to " + LGJob.getStatusString(newStatus)
                    + " reason:" + reason +" for job:"+jobId);
        }
        return true;
    }

    //Updates endTime and totalRunningTimeSeconds for LGJob and job in MongoDB.
    public static LGJob setEndTime(LGJob job) {
        String jobId = job.getJobId();
        long endTime = System.currentTimeMillis();
        job.setEndTime(endTime);
        long runningTime = job.calculateCurrentRunningTimeSeconds();
        job.setTotalRunningTimeSeconds(runningTime);
        open();
        MONGO_DB_STORE.appendToJob(jobId, "endTime", endTime);;//updates the field in mongoDB
        MONGO_DB_STORE.appendToJob(jobId, "totalRunningTimeSeconds", runningTime);;//updates the field in mongoDB
        return job;
    }

    //Updates endTime and totalRunningTimeSeconds for LGJob and job in MongoDB.
    public static LGJob setStartTime(LGJob job) {
        String jobId = job.getJobId();
        long startTime = System.currentTimeMillis();
        job.setStartTime(startTime);
        open();
        MONGO_DB_STORE.appendToJob(jobId, "startTime", startTime);;//updates the field in mongoDB
        return job;
    }

    //active meaning not FINISHED/FINISHED_WITH_ERRORS/STOPPED
    public List<LGJob> getAllActive() {
        open();
        return JOB_DAO.getAllActive();
    }

    public List<LGJob> getAllProcessing() {
        open();
        return JOB_DAO.getAllProcessing();
    }

    public List<LGJob> getAllNew() {
        open();
        return JOB_DAO.getAllNew();
    }

    public List<LGJob> getAllError() {
        open();
        return JOB_DAO.getAllError();
    }

    /**
     * Checks if active task count for job is 0. Sets job to finished if it is, and returns true; else returns false;
     * Also sets endTime and totalRunningSeconds
     * @param job LGJob
     * @return true if job is finished and gets updated, false otherwise
     * */
    public static boolean updateJobIfFinished(LGJob job) {
        if (job.getActiveTaskCount() == 0) {
            setJobFinished(job);
            return true;
        }
        return false;
    }

    /**
     * If a task is marked as FAILED, it's considered FINISHED however, the job status won't be finished,
     * it will be STATUS_FINISHED_WITH_ERRORS
     *
     * @param job The jobId of the job to set to FINISHED
     */
    public static void setJobFinished(LGJob job) {
        if (job == null) {return;}
        String jobId = job.getJobId();
        int failedTaskCount = job.getFailedTaskCount();
        String jobHistoryMessage = "job FINISHED";
        int status = LGJob.STATUS_FINISHED;

        //Error case
        if(failedTaskCount > 0 || job.getStatus() == LGJob.STATUS_ERROR) {
            status = LGJob.STATUS_FINISHED_WITH_ERRORS;
            jobHistoryMessage = "job FINISHED_WITH_ERRORS";
            if(failedTaskCount == 0) {
                LGJobError error = new LGJobError(jobId, "No Task", "Job Error", "No Adapter", "Unknown error.");
                updateErrorsForJob(job, error);
            }
        }

        log.info("Setting job:" + jobId + " to status " + LGJob.getStatusString(status));
        LGJobHistory historyLine = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND, "state_change", "",
                jobHistoryMessage, System.currentTimeMillis(), System.currentTimeMillis(), 0, 0, 0, 0);
        updateJobHistory(job, historyLine);
        setEndTime(job); //sets endTime and totalRunningTimeSeconds on LGJob and in mongo
        setStatus(job, status);//sets job status on LGJOb and in mongo
    }

    public boolean hasJobExpired(LGJob lg) {
        // 0 means run forever
        if (lg.getTTL() <= 0) {
            return false;
        }
        long runTime = (System.currentTimeMillis() - lg.getStartTime())/1000;
        if (runTime > lg.getTTL()) {
            log.warn("Job:"+lg.getJobId()+" has EXPIRED");
            setStatus(lg, LGJob.STATUS_EXPIRED);
            return true;
        }
        return false;
    }

    public static void addTaskToJob(LGJob job, LGTask task) {
        open();
        JOB_DAO.saveTask(task);
        boolean success;
        success = setStatus(job, LGJob.STATUS_PROCESSING);//update status to processing
        if(success == false) {
            log.warn("Problem setting Job " + job.getJobId() + " status to PROCESSING");
        }
        job.addTask(task);//updates lastTaskTime and taskMap for LGJob
        MONGO_DB_STORE.appendToJob(job.getJobId(), "lastTaskTime", job.getLastTaskTime());//updates lastTaskTime in mongo
        DBRef taskRef = new DBRef("tasks", task.getTaskId());
        JSONObject taskEntry = new JSONObject()
                .put(task.getTaskId(), taskRef)
                ;
        MONGO_DB_STORE.insertToObject("jobs", task.getJobId(), "taskMap", taskEntry);
    }

    public static void updateJobHistorySuccess(LGJob job, String taskId, CoordinatorBolt.MetricData md) {
        updateJobHistorySuccess(job, taskId, md.graphChanges, md.maxGraphId, md.numberOfNewTasksGenerated, md.currentGraphId);
    }

    public static void updateJobHistorySuccess(LGJob job, String taskId, int graphChanges, int maxGraphId, int numberOfNewTasksCreated, int currentId) {
        if (job == null || (taskId == null) || (taskId.equals("") || job.getJobId().equals(""))){
            return;  // Most likely a seed job.?
        }
        open();
        LGTask lgt = TASK_DAO.getByTaskId(taskId);
        if (lgt == null) { return; }

        LGJobHistory history = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_TASK, "adapter", taskId,
                lgt.getAdapterName() + " completed task",
                lgt.getStartTime(), System.currentTimeMillis(), graphChanges, maxGraphId
                , numberOfNewTasksCreated, currentId);
        updateJobHistory(job, history);
    }

    public static void updateJobHistoryFailed(String jobId, String taskId) {
        LGJob job = getJob(jobId);
        if (job == null) { return; }
        LGTask lgt = TASK_DAO.getByTaskId(taskId);
        if (lgt == null) { return; }

        LGJobHistory history = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_TASK,  "adapter", taskId,
                lgt.getAdapterName() + " FAILED task",
                lgt.getStartTime(), System.currentTimeMillis(),0,0,0,0);
        updateJobHistory(job, history);
    }

    /**
     * Updates an LGJob's history field and updates the jobHistory entry in mongodb
     * @param job LGJob
     * @param history LGJobHistory
     */
    public static void updateJobHistory(LGJob job, LGJobHistory history) {
        open();
        job.addHistory(history);
        JSONObject jHistory = history.toJson();
        MONGO_DB_STORE.appendToList("jobs", job.getJobId(), "jobHistory", jHistory);
    }

    /** Update graphActivity Value
     * @param job LGJob
     * @param graphActivity Int graph activity
     * */
    public static void updateGraphActivity(LGJob job, int graphActivity) {
        if (job.getGraphActivity() < graphActivity) {
            open();
            job.setGraphActivity(graphActivity);
            MONGO_DB_STORE.appendToJob(job.getJobId(), "graphActivity", graphActivity);
        }
    }

    /**
     * @param jobId String job id
     * @param error LGJobError
     */
    public static void updateMaxGraphSizeErrorsForJob(String jobId, LGJobError error) {
        LGJob job = getJob(jobId);
        boolean found = false;
        List<LGJobError> existingJobErrorsforThisJobId = job.getJobErrors();
        for (LGJobError jobError : existingJobErrorsforThisJobId) {
            if (jobError.getAdapter().equals(error.getAdapter())) {
                log.info("Max_Graph_Error has already been reported, will continue without reporting again.");
                found = true;
                break;
            }
        }
        if(!found) {updateErrorsForJob(job, error);}
    }

    /**
     * Handling new job errors
     * @param job LGJob
     * @param error LGJobError
     */
    public static void updateErrorsForJob(LGJob job, LGJobError error) {
        open();
        job.addError(error);
        JSONObject jError = error.toJson();
        MONGO_DB_STORE.appendToList("jobs", job.getJobId(), "jobErrors", jError);
    }

    /**
     * Adds an entry to jobs to "taskErrorsMap"to jobId:[]
     * @param job LGJob
     * @param taskId String for task ID
     * @param error LGJobError
     * @throws NullPointerException thrown when failing to add task for valid LGJob
     */
    public void updateErrorsForTask(LGJob job, String taskId, LGJobError error) throws NullPointerException {
        if(job != null && !job.getJobId().equals("") && !taskId.equals("")) {
            open();
            job.addTaskError(taskId, error);
            MONGO_DB_STORE.appendToList("jobs", job.getJobId(), "taskErrorsMap." + taskId, error.toJson());
        }
    }


    /**
     * Remove all errors from errorMap by TaskId
     * @param job LGJob
     * @param taskId String for task ID
     */
    public static void removeErrorsForJobByTaskId(LGJob job, String taskId) {
        open();
        job.removeErrorsByTaskId(taskId);
        JSONObject item = new JSONObject().put("taskId", taskId);
        MONGO_DB_STORE.removeFromList("jobs", job.getJobId(), "jobErrors", item);
    }

    /**
     * This should only ever be called when DELETING a JOB from the database
     * @param jobId String for job ID
     * @param taskId String for task ID
     */
     protected void removeTaskFromJob(String jobId, String taskId) {
        LGJob job = getJob(jobId);
        if (job == null) { return; }
        LGTask lgt = TASK_DAO.getByTaskId(taskId);
        if (lgt == null) { return; }
        JOB_DAO.deleteTaskFromJob(job, lgt);
    }

    /**
     * When a task is done running (we received the response back from the adapter) we mark the task status.
     * Note, a Job is considered "FINISHED' if there are no outstanding tasks in the database for that job.
     * Status can be LGTask.TASK_STATUS_COMPLETE or LGTask.TASK_STATUS_FAILED...
     * @param job LGJob
     * @param taskId String for task ID
     * @param status Int for status
     */
    public static void updateJobTaskStatus(LGJob job, String taskId, int status) throws NullPointerException {
        String jobId = null;
        try {
            jobId = job.getJobId();
            if (job == null || (taskId == null) || (taskId.equals("") || job.getJobId().equals(""))) {
                return;  // Most likely a seed job.?
            }
            open();
            LGTask lgt = TASK_DAO.getByTaskId(taskId);
            if (lgt == null) {
                log.error("Error in updateJobTaskToStatus for job_id:" + jobId + "  task_id:" + taskId
                        + " Unable to find task in database.");
                return;
            }

            job.updateTaskStatus(taskId, status);
            lgt.setStatus(status);
            long endTime = System.currentTimeMillis();
            lgt.setEndTime(endTime);
            job.updateTask(lgt);
            TASK_DAO.update(taskId, "status", status);
            TASK_DAO.update(taskId, "endTime", endTime);
        }
        catch(Exception e) {
            log.error("Failed to update task status for job:" + jobId + " task:" + taskId + " to "+LGJob.getStatusString(status)+".");
            e.printStackTrace();
        }
    }


    /**
     * Update the task status to COMPLETE
     * @param job LGJob
     * @param taskId String for task ID
     */
    public static void updateTaskToCompleted(LGJob job, String taskId) {
        updateJobTaskStatus(job, taskId, LGTask.TASK_STATUS_COMPLETE);
    }

    /**
     * Update the task status to FAILED
     * @param job LGJob
     * @param taskId String for task ID
     */
    public static void updateTaskToFailed(LGJob job, String taskId) {
        updateJobTaskStatus(job, taskId, LGTask.TASK_STATUS_FAILED);
    }

    /**
     * Update the task status to DROPPED
     * @param job LGJob
     * @param taskId String for task ID
     */
    public static void updateTaskToDropped(LGJob job, String taskId) {
        updateJobTaskStatus(job, taskId, LGTask.TASK_STATUS_DROPPED);
    }

    /**
     * @param taskId String for task ID
     * @return LGTask returned
     */
    public static LGTask getTask(String taskId) {
        open();
        return TASK_DAO.getByTaskId(taskId);
    }

    /**
     * @param status String for status
     * @return Returns list of LGJob items
     */
    public List<LGJob> getAllByStatus(String status) {
        open();
        return JOB_DAO.getAllByStatus(status);
    }

    /**
     * @param beforeDate Date for before data
     * @param afterDate Date for after date
     * @return Returns list of LGJob items
     */
    public List<LGJob> getAllByDateRange(Date beforeDate, Date afterDate) {
        open();
        return JOB_DAO.getAllByDateRange(beforeDate, afterDate);
    }

    /**
     * @return Retuns List of LGJob items
     */
    public List<LGJob> getAll() {
        open();
        return JOB_DAO.getAll();
    }

    /**
     * @param status String status
     * @param reason String reason
     * @return Returns list of LGJob items
     */
    public List<LGJob> getAllByStatusAndReason(String status, String reason) {
        open();
        return JOB_DAO.getAllByStatusAndReason(status, reason);
    }

    /**
     * @param fdays int
     * @param tdays int
     * @return Returns a List of LGJob items
     */
    public List<LGJob> getAllByDays(int fdays, int tdays) {
        open();
        return JOB_DAO.getAllByDays(fdays, tdays);
    }

    /**
     * @param days int
     * @return Returns List of LGJob items
     */
    public List<LGJob> getAllByOlderThanDays(int days) {
        open();
        return JOB_DAO.getAllByOlderThanDays(days);
    }

    /**
     * @param mins int
     * @param tomins int
     * @return Returns List of LGJob items
     */
    public List<LGJob> getAllByMins(int mins, int tomins) {
        open();
        return JOB_DAO.getAllByMins(mins, tomins);
    }

    /**
     * @param days int
     * @return Returns List of LGJob items
     */
    public List<LGJob> getAllByAge(int days) {
        open();
        return JOB_DAO.getAllByAge(days);
    }

    /**
     * @param id String for ID
     * @param status String for status
     * @param reason String for reason
     * @return List of LGJob items
     */
    public List<LGJob> getAllLimitFields(String id, String status, String reason) {
        open();
        return JOB_DAO.getAllLimitFields(id, status, reason);
    }

    /**
     * @param count int
     * @return List of LGJob items
     */
    public List<LGJob> getLast(int count) {
        open();
        return JOB_DAO.getLast(count);
    }

    /**
     * @param dbValue String for dbValue
     * @return JSONArray
     */
    public JSONArray getAllJobsThatHaveDbValueKeyJSONArray(String dbValue) {
        open();
        return DBVALUE_DAO.getAllJobsThatHaveDbValueKeyJSONArray(dbValue.toLowerCase());
    }

    /**
     * @param jobId String of job ID
     * @param key String for key
     * @return LGdbValue
     */
    public LGdbValue getDbValuesByJobIdandKey(String jobId, String key) {
        open();
        return DBVALUE_DAO.getDbValuesByJobIdandKey(jobId, key);
    }

    /**
     * @param jobId String for job ID
     * @return LGdbValue
     */
    public LGdbValue getDbValuesByJobId(String jobId) {
        open();
        return DBVALUE_DAO.getDbValuesByJobId(jobId);
    }

    /**
     * @param args Unused. Standard main function for testing.
     */
    public static void main(final String[] args) {
        JobManager jobManager = new JobManager();
        String jobId = "35cdb3d4-035b-11e7-a37f-000000000000";
        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date dateObj = new Date();
        String currentDate = df.format(dateObj);
        LGJob job = new LGJob();
        job.setJobId(jobId);
        String configString = "{}";
        JSONObject job_config = new JSONObject(configString);
        job.setJobConfig(configString);
        jobManager.setStatus(job, LGJob.STATUS_FINISHED, "Test Reason. Date:" + currentDate, false);
        jobManager.updateJobConfig(job, job_config);
        jobManager.close();
    }
}