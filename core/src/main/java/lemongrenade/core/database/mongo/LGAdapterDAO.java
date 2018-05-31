package lemongrenade.core.database.mongo;

import lemongrenade.core.models.LGAdapterModel;
import org.bson.types.ObjectId;
import org.mongodb.morphia.dao.DAO;
import java.util.List;

public interface LGAdapterDAO extends DAO<LGAdapterModel, ObjectId> {
    LGAdapterModel getById(String id);
    List<LGAdapterModel> getAll();
}