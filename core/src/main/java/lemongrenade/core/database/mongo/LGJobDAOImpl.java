package lemongrenade.core.database.mongo;

import com.mongodb.WriteResult;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGTask;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import java.util.*;

public class LGJobDAOImpl extends BasicDAO<LGJob, ObjectId>
implements LGJobDAO
{
    private LGTaskDAO taskDAO;
    public LGJobDAOImpl(Class<LGJob> entityClass, Datastore ds) {
        super(entityClass, ds);
        taskDAO = new LGTaskDAOImpl(LGTask.class, ds);
    }

    public LGJob getByJobId(String jobId) {
        Query<LGJob> query = createQuery().field("_id").equal(jobId);
        return query.get();
    }

    public void saveTask(LGTask lgTask) {
        taskDAO.save(lgTask);
    }

    public List<LGJob> getAll() {
        return getDatastore().createQuery(LGJob.class).asList();
    }

    public List<LGJob> getLast(int count) {
        Query<LGJob> query = getDatastore().createQuery(LGJob.class)
                .order("-createDate")
                .limit(count);
        return query.asList();
    }

    public List<LGJob> getAllByDays(int fromDays, int toDays) {
        if (fromDays <= 0) { fromDays = 1; }
        if (toDays   <= 0) { toDays = 1; }
        Calendar fromDate = Calendar.getInstance();
        Calendar toDate = Calendar.getInstance();
        fromDays *= -1; toDays *= -1;
        fromDate.add(Calendar.DAY_OF_YEAR, fromDays);
        toDate.add(Calendar.DAY_OF_YEAR, toDays);
        Query<LGJob> query = getDatastore().createQuery(LGJob.class)
                .filter("createDate <=", fromDate.getTime())
                .filter("createDate >=",toDate.getTime());
        return query.asList();
    }

    public List<LGJob> getAllByMins(int fromMins, int toMins) {
        Calendar fromDate = Calendar.getInstance();
        Calendar toDate = Calendar.getInstance();
        fromMins *= -1;
        toMins *= -1;
        fromDate.add(Calendar.MINUTE, fromMins);
        toDate.add(Calendar.MINUTE, toMins);
        System.out.println("DATES "+fromDate.toString()+"  "+toDate.toString());
        Query<LGJob> query = getDatastore().createQuery(LGJob.class)
                .filter("createDate <=", fromDate.getTime())
                .filter("createDate >=",toDate.getTime());
        return query.asList();
    }

    public List<LGJob> getAllByAge(int days) {
        if (days <= 0) {
            days = 1;
        }
        Calendar cdate = Calendar.getInstance();
        days *= -1;
        cdate.add(Calendar.DAY_OF_YEAR, days);
        Query<LGJob> query = getDatastore().createQuery(LGJob.class).filter("createDate >=", cdate.getTime());
        return query.asList();
    }

    public List<LGJob> getAllActive() {
        Query<LGJob> q = getDatastore().createQuery(LGJob.class);
        q.field("status").equal(LGJob.STATUS_PROCESSING);
        return q.asList();
    }

    public List<LGJob> getAllByStatus(String status) {
        int lv = LGJob.getStatusByString(status);
        if (lv == 0) {
            lv = LGJob.STATUS_PROCESSING;    // If unknown, give them active
        }

        Query<LGJob> q = getDatastore().createQuery(LGJob.class);
        q.field("status").equal(lv);
        return q.asList();
    }

    public List<LGJob> getAllByStatusAndReason(String status, String reason) {
        int lv = LGJob.getStatusByString(status);
        if (lv == 0) {
            lv = LGJob.STATUS_PROCESSING;    // If unknown, give them active
        }
        Query<LGJob> q = getDatastore().createQuery(LGJob.class);
        q.field("status").equal(lv)
                .field("reason").equal(reason);
        return q.asList();
    }

    public void deleteTaskFromJob(LGJob jobIn, LGTask task) {
        LGJob job = getByJobId(jobIn.getJobId());
        // Need to remove the reference first!
        job.delTask(task.getTaskId());
        getDatastore().save(job);
        taskDAO.delete(task);
    }

    public void update(String jobId, String var, String value) {
        Query<LGJob> query = createQuery().field("_id").equal(jobId);
        UpdateOperations<LGJob> ops = getDatastore().createUpdateOperations(LGJob.class).set(var, value);
        getDatastore().update(query, ops);
    }

    public void updateInt(String jobId, String var, int value) {
        Query<LGJob> query = createQuery().field("_id").equal(jobId);
        UpdateOperations<LGJob> ops = getDatastore().createUpdateOperations(LGJob.class).set(var, value);
        getDatastore().update(query, ops);
    }

    @Override
    public WriteResult delete(LGJob inJob) {
        LGJob lgjob = getByJobId(inJob.getJobId());
        Map<String, LGTask> taskMap = lgjob.getTaskMap();
        for (Map.Entry<String, LGTask> entry : taskMap.entrySet()) {
            LGTask lgt = entry.getValue();
            this.deleteTaskFromJob(lgjob,lgt);
        }
        return super.delete(lgjob);
    }

    public static void main(String args[]) {
        LGJobDAOImpl   dao;
        MorphiaService ms;
        ms = new MorphiaService();
        dao = new LGJobDAOImpl(LGJob.class, ms.getDatastore());

        String jobId = UUID.randomUUID().toString();
        ArrayList<String> alist = new ArrayList<>();
        alist.add("HelloWorld");
        alist.add("PlusBang");
        alist.add("HelloWorldPython");
        alist.add("LongRunningTestAdapter");
        alist.add("LongRunningPython");

        JSONObject jobConfig = new JSONObject();
        jobConfig.put("job_id",jobId);
        jobConfig.put("ttl",300); // 5 mins
        jobConfig.put("depth",3);
        jobConfig.put("description","test job");

        // Create test job
        LGJob testJob = new LGJob(jobId, alist, jobConfig);
        dao.save(testJob);

        System.out.println("Query Job") ;
        long start = System.currentTimeMillis();
        LGJob lookup = dao.getByJobId(jobId);
        System.out.println(lookup.toString());
        System.out.println("  Depth "+lookup.getDepth());
        long seconds = (System.currentTimeMillis() - start) ;
        System.out.println("Query complete. Seconds :"+seconds);
    }
}
