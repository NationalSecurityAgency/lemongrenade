package lemongrenade.core.database.mongo;

import com.mongodb.WriteResult;
import lemongrenade.core.models.LGdbValue;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.mongodb.morphia.dao.DAO;

import java.util.ArrayList;

public interface LGdbValueDAO extends DAO<LGdbValue, ObjectId> {
    LGdbValue getDbValuesByJobIdandKey(String jobId, String key);
    LGdbValue getDbValuesByJobId(String jobId);
    WriteResult delete(String jobId);
    void saveDbValues(LGdbValue LGdbValue);
    void addDbValueToJob(String jobId, String key, String dbValue) ;
    void addDbValueToJob(String jobId, String key, JSONArray dbValues);
    JSONArray getAllJobsThatHaveDbValueKeyJSONArray(String dbValueKey);
    ArrayList<LGdbValue> getAllJobsThatHaveDbValueKey(String dbValueKey);
}