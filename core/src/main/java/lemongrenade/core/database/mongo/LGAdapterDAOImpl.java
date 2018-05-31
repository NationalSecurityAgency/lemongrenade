package lemongrenade.core.database.mongo;

import lemongrenade.core.models.LGAdapterModel;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;

import java.util.List;


public class LGAdapterDAOImpl extends BasicDAO<LGAdapterModel, ObjectId>
        implements LGAdapterDAO {


    public LGAdapterDAOImpl(Class<LGAdapterModel> entityClass, Datastore ds) {
        super(entityClass, ds);
    }

    /** */
    public LGAdapterModel getById(String id) {
        Query<LGAdapterModel> query = createQuery().field("_id").equal(id);
        return query.get();
    }

    /** */
    public List<LGAdapterModel> getAll() {
        return getDatastore().createQuery(LGAdapterModel.class).asList();
    }
}