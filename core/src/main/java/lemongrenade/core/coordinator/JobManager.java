package lemongrenade.core.coordinator;

import lemongrenade.core.database.mongo.*;
import lemongrenade.core.models.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class JobManager {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private LGJobDAO lgjobDAO;
    private LGTaskDAO lgtaskDAO;
    private MorphiaService ms;

    // Constructor
    public JobManager() {
        ms = new MorphiaService();
        lgjobDAO = new LGJobDAOImpl(LGJob.class, ms.getDatastore());
        lgtaskDAO= new LGTaskDAOImpl(LGTask.class, ms.getDatastore());
    }

    public void addJob(LGJob job) {
        lgjobDAO.save(job);
    }

    public LGJob getJob(String jobId) {
        return lgjobDAO.getByJobId(jobId);
    }

    public boolean deleteJob(LGJob job)
    {
        if (job.getStatus() == LGJob.STATUS_PROCESSING) {
            log.error("Delete Failed: job is in processing state."+job.getJobId());
            return false;
        }
        lgjobDAO.delete(job);
        return true;
    }

    public boolean doesJobExist(String jobId) {
        LGJob j = this.getJob(jobId);
        if (j == null) {
            return false;
        }
        return true;
    }
    public boolean doesJobExist(LGJob job) { return doesJobExist(job.getJobId()); }

    public boolean updateJobConfig(LGJob job, JSONObject config) {
        job.setJobConfig(config.toString());
        lgjobDAO.save(job);
        return true;
    }

    public boolean setStatus(LGJob job, int status) {
        boolean hasStatusChanged = false;
        try {
            hasStatusChanged = job.setStatus(status);
        }
        catch (InvalidJobStateChangeException e) {
            log.error("Invalid job state change from "+job.getStatusString(job.getStatus())+" to "+job.getStatusString(status));
            return false;
        }
        // Only update if status has changed
        if (hasStatusChanged) {
            lgjobDAO.updateInt(job.getJobId(), "status",  status);
        }
        return true;
    }

    /** Some states(RESET) we store a state value also */
    public boolean setStatus(LGJob job, int status, String reason) {
        boolean hasStatusChanged = false;
        try {
            hasStatusChanged = job.setStatus(status);
        }
        catch (InvalidJobStateChangeException e) {
            log.error("Invalid job state change from "+job.getStatusString(job.getStatus())+" to "+job.getStatusString(status));
            return false;
        }
        // Only update if status has changed
        if (hasStatusChanged) {
            lgjobDAO.update(job.getJobId(),"reason", reason);
            lgjobDAO.updateInt(job.getJobId(), "status", status);
        }
        return true;
    }

    public List<LGJob> getAllActive() {
        return lgjobDAO.getAllActive();
    }

    /**
     * If this job doesn't have any more "active" tasks. An active tasks is any task that is
     * marked "Completed:false"
     *
     * If a task is marked as FAILED, it's considered FINISHED however, the job status won't be finished,
     * it will be STATUS_FINISHED_WITH_ERRORS
     *
     */
    public LGJob checkIfJobFinished(String jobId) {
        LGJob job = getJob(jobId);

        int failedTaskCount = job.getFailedTaskCount();

        if (job.getActiveTaskCount() == 0)  {
            String jobHistoryMessage = "job FINISHED";
            if (failedTaskCount == 0) {
                log.info("Setting job:" + job.getJobId() + " to status FINISHED");
                this.setStatus(job, LGJob.STATUS_FINISHED);
            } else {
                log.warn("Setting job: "+job.getJobId() + " to status FINISHED_WITH_ERRORS");
                this.setStatus(job, LGJob.STATUS_FINISHED_WITH_ERRORS);
                jobHistoryMessage = "job FINISHED_WITH_ERRORS";
            }
            LGJobHistory historyLine = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_COMMAND, "state_change", "",
                    jobHistoryMessage, System.currentTimeMillis(), System.currentTimeMillis(), 0, 0, 0, 0);
            updateHistoryForJob(job.getJobId(), historyLine);
        }
        return job;
    }

    public boolean hasJobExpired(LGJob lg) {
        // 0 means run forever
        if (lg.getTTL() <= 0) {
            return false;
        }
        long runTime = (System.currentTimeMillis() - lg.getStartTime())/1000;
        if (runTime > lg.getTTL()) {
            log.warn("Job:"+lg.getJobId()+" has EXPIRED");
            this.setStatus(lg,LGJob.STATUS_EXPIRED);
            return true;
        }
        return false;
    }

    public void addTaskToJob(LGJob job, LGTask task) {
        lgjobDAO.saveTask(task);
        try {
            job.setStatus(LGJob.STATUS_PROCESSING);
        }
        catch (InvalidJobStateChangeException e) {
            log.warn("Problem setting Job "+job.getJobId()+" status to PROCESSING");
        }
        job.addTask(task);
        lgjobDAO.save(job);
    }

    public void updateHistoryAdapterForJob(String jobId, String taskId, int graphChanges, int maxGraphId,
                                           int numberOfNewTasksCreated, int currentId) {
        LGJob job = getJob(jobId);
        if (job == null) { return; }
        LGTask lgt = lgtaskDAO.getByTaskId(taskId);
        if (lgt == null) { return; }

        LGJobHistory h = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_TASK, "adapter", taskId,
                lgt.getAdapterName() + " completed task",
                lgt.getStartTime(), System.currentTimeMillis(), graphChanges, maxGraphId
                , numberOfNewTasksCreated, currentId);
        job.addHistory(h);
        lgjobDAO.save(job);
    }

    public void updateHistoryAdapterFailedForJob(String jobId, String taskId) {
        LGJob job = getJob(jobId);
        if (job == null) { return; }
        LGTask lgt = lgtaskDAO.getByTaskId(taskId);
        if (lgt == null) { return; }

        LGJobHistory h = new LGJobHistory(LGJobHistory.LGHISTORY_TYPE_TASK,  "adapter", taskId,
                lgt.getAdapterName() + " FAILED task",
                lgt.getStartTime(), System.currentTimeMillis(),0,0,0,0);
        job.addHistory(h);
        lgjobDAO.save(job);
    }

    /** Update graphActivity Value */
    public void updateGraphActivity(String jobId, int graphActivity) {
        LGJob job = getJob(jobId);
        if (job.getGraphActivity() < graphActivity) {
            job.setGraphActivity(graphActivity);
            lgjobDAO.save(job);
        }
    }

    /** New History update method that accepts history command types*/
    public void updateHistoryForJob(String jobId, LGJobHistory historyLine) {
        LGJob job = getJob(jobId);
        job.addHistory(historyLine);
        lgjobDAO.save(job);
    }

    /** Handle new job errors*/
    public void updateErrorsForJob(String jobId, LGJobError error) {
        LGJob job = getJob(jobId);
        job.addError(error);
        lgjobDAO.save(job);
    }

    /**
     * This should only ever be called when DELETING a JOB from the datbase
     * */
    protected void removeTaskFromJob(String jobId, String taskId) {
        LGJob job = getJob(jobId);
        if (job == null) { return; }
        LGTask lgt = lgtaskDAO.getByTaskId(taskId);
        if (lgt == null) { return; }
        lgjobDAO.deleteTaskFromJob(job, lgt);
    }

    /**
     * When a task is done running (we received the response back from the adapter) we mark the task status.
     * Note, a Job is considered "FINISHED' if there are no outstanding tasks in the database for that job.
     * Status can be LGTask.TASK_STATUS_COMPLETE or LGTask.TASK_STATUS_FAILED...
     * @param jobId
     * @param taskId
     */
    public void updateJobTaskStatus(String jobId, String taskId, int status) {
        LGJob job = getJob(jobId);
        if (job == null) { return; }
        LGTask lgt = lgtaskDAO.getByTaskId(taskId);
        if (lgt == null) {
            log.error("Error in updateJobTaskToStatus for job_id:"+jobId+ "  task_id:"+taskId
                +" Unable to find task in database.");
            return;  // TODO: throw exception?
        }

        lgt.setStatus(status);
        long endTime = System.currentTimeMillis();
        lgt.setEndTime(endTime);
        lgtaskDAO.update(taskId,"status",status);
        lgtaskDAO.update(taskId,"endTime",endTime);
    }

    public LGTask getTask(String taskId) {
        return lgtaskDAO.getByTaskId(taskId);
    }

    public static void main(final String[] args) throws UnknownHostException {
        JobManager jm = new JobManager();
        String id = "001";
        ArrayList<String> alist = new ArrayList<>();
        alist.add("adapter1");
        alist.add("adapter2");
        alist.add("adapter3");
        JSONObject jobConfig = new JSONObject();
        LGJob job = new LGJob(id, alist, jobConfig);
        jm.addJob(job);
        System.out.println("Job Added");
        LGJob job2 = jm.getJob(id);
        System.out.println("Lookup job "+job2.toString());
    }
}