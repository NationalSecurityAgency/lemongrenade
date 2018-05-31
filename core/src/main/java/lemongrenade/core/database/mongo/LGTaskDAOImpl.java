package lemongrenade.core.database.mongo;

import lemongrenade.core.models.LGTask;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

public class LGTaskDAOImpl extends BasicDAO<LGTask, ObjectId>
implements LGTaskDAO {
    public LGTaskDAOImpl(Class<LGTask> entityClass, Datastore ds) {
        super(entityClass, ds);
    }

    public LGTask getByTaskId(String taskId) {
        Query<LGTask> query = createQuery().field("_id").equal(taskId);
        return query.get();
    }

    //Updates an existing value in the task. Throws an error if "var" doesn't exist.
    public void update(String taskId, String var, String value) {
        Query<LGTask> query = createQuery().field("_id").equal(taskId);
        UpdateOperations<LGTask> ops = getDatastore().createUpdateOperations(LGTask.class).set(var, value);
        getDatastore().update(query, ops);
    }

    public void updateInt(String taskId, String var, int value) {
        Query<LGTask> query = createQuery().field("_id").equal(taskId);
        UpdateOperations<LGTask> ops = getDatastore().createUpdateOperations(LGTask.class).set(var, value);
        getDatastore().update(query.disableValidation(), ops);
    }

    public void update(String taskId, String var, boolean value) {
        Query<LGTask> query = createQuery().field("_id").equal(taskId);
        UpdateOperations<LGTask> ops = getDatastore().createUpdateOperations(LGTask.class).set(var, value);
        getDatastore().update(query, ops);
    }

    public void update(String taskId, String var, long value) {
        Query<LGTask> query = createQuery().field("_id").equal(taskId);
        UpdateOperations<LGTask> ops = getDatastore().createUpdateOperations(LGTask.class).set(var, value);
        getDatastore().update(query, ops);
    }

    public void saveTask(LGTask lgTask) {
        getDatastore().save(lgTask);
    }
    public static void main(final String[] args) {
        String taskId = "9881d398-1ce2-4c7e-8309-1d4e07ff0a26";
        MorphiaService ms = new MorphiaService();
        LGTaskDAO lgtaskDAO = new LGTaskDAOImpl(LGTask.class, ms.getDatastore());
        lgtaskDAO.update(taskId, "parentTaskId", "testValue");
    }
}