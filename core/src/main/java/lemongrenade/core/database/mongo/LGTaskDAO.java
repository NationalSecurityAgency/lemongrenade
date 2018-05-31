package lemongrenade.core.database.mongo;

import lemongrenade.core.models.LGTask;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.DAO;

public interface LGTaskDAO extends DAO<LGTask, ObjectId> {
    public LGTask getByTaskId(String taskId);
    public void saveTask(LGTask lgTask);
    void update(String id, String var, boolean value);
    void update(String id, String var, long value);
    void update(String taskId, String var, String value);
}