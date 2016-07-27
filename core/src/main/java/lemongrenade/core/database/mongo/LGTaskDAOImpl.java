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

    public void update(String taskId, String var, String value) {
        Query<LGTask> query = createQuery().field("_id").equal(taskId);
        UpdateOperations<LGTask> ops = getDatastore().createUpdateOperations(LGTask.class).set(var, value);
        getDatastore().update(query, ops);
    }

    public void updateInt(String taskId, String var, int value) {
        Query<LGTask> query = createQuery().field("_id").equal(taskId);
        UpdateOperations<LGTask> ops = getDatastore().createUpdateOperations(LGTask.class).set(var, value);
        getDatastore().update(query, ops);
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
}