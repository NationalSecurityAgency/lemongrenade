package lemongrenade.core.database.mongo;

import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGTask;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.mongodb.morphia.dao.DAO;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

public interface LGJobDAO extends DAO<LGJob, ObjectId> {
    LGJob getByJobId(String jobId);
    void saveTask(LGTask lgTask);
    List<LGJob> getAll();
    void update(String id, String var, String value);
    void updatePush(String id, String var, String value);
    void updateInt(String id, String var, int value);
    List<LGJob> getAllActive();
    void deleteTaskFromJob(LGJob jobIn, LGTask task);
    List<LGJob> getAllProcessing();
    List<LGJob> getAllNew();
    List<LGJob> getAllError();
    HashMap<String, LGJob> getByJobIds(JSONArray jobIds);
    List<LGJob> getAllByStatus(String status);
    List<LGJob> getAllByDateRange(Date beforeDate, Date afterDate);
    List<LGJob> getAllByStatusAndReason(String status, String reason);
    List<LGJob> getAllByDays(int fdays, int tdays);
    List<LGJob> getAllByOlderThanDays(int days);
    List<LGJob> getAllByMins(int mins, int tomins);
    List<LGJob> getAllByAge(int days);
    List<LGJob> getLast(int count);
}