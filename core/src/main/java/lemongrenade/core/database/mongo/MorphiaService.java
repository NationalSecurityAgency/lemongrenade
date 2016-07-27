package lemongrenade.core.database.mongo;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import lemongrenade.core.util.LGProperties;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphiaService {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private Morphia morphia;
    private Datastore datastore;

    public MorphiaService(){
        String connectString = LGProperties.get("database.mongo.hostname")+":"+ LGProperties.get("database.mongo.port");
        log.info("Mongo connection :"+connectString);
        MongoClient mongoClient = new MongoClient(connectString);
        mongoClient.setWriteConcern(WriteConcern.JOURNALED);
        this.morphia = new Morphia();
        String databaseName = LGProperties.get("database.mongo.databasename");
        if (databaseName == null) {
            databaseName = "lemongrenade_develop";
            log.error("Invalid database name is in properties file. Defaulting to "+databaseName);
        }
        this.datastore = morphia.createDatastore(mongoClient, databaseName);
        //this.datastore.setDefaultWriteConcern(WriteConcern.ACKNOWLEDGED);
        this.datastore.setDefaultWriteConcern(WriteConcern.JOURNALED);
    }


    public Morphia getMorphia() {
        return morphia;
    }

    public void setMorphia(Morphia morphia) {
        this.morphia = morphia;
    }

    public Datastore getDatastore() {
        return datastore;
    }

    public void setDatastore(Datastore datastore) {
        this.datastore = datastore;
    }
}
