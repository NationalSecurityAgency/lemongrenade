package lemongrenade.core.database.mongo;

import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGTask;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.DAO;
import java.util.List;

public interface LGJobDAO extends DAO<LGJob, ObjectId> {
    LGJob getByJobId(String jobId);
    void saveTask(LGTask lgTask);
    List<LGJob> getAll();
    void update(String id, String var, String value);
    void updateInt(String id, String var, int value);
    List<LGJob> getAllActive();
    void deleteTaskFromJob(LGJob jobIn, LGTask task);
}