package lemongrenade.core.database.mongo;

import com.mongodb.WriteResult;
import lemongrenade.core.models.LGAdapterURLs;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.mongodb.morphia.dao.DAO;


public interface LGAdapterURLsDAO extends DAO<LGAdapterURLs, ObjectId> {

    public WriteResult delete(String adapter);
    public void saveNewBaseURL (String adapter, String baseURL);
    public void saveNewMultipleBaseURLs(String body);
    public String getBaseUrlByAdapter(String adapter);
    public LGAdapterURLs getLGAdapterURLByAdapter(String adapter);
    public JSONObject getAllJSON();


}