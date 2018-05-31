package lemongrenade.core.database.mongo;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import lemongrenade.core.util.LGProperties;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MorphiaService {

    static private final Logger log = LoggerFactory.getLogger(MorphiaService.class);
    static private Morphia morphia;
    static private Datastore datastore;
    static private MongoClient mongoClient;

    static {
        //Init all private variables
        morphia = new Morphia();
        open();
    }

    //Close the client then null client/datastore if close hasn't already been called
    public void close () {
        if(mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
            datastore = null;
        }
    }

    //Open the connection if datastore or mongoClient is null
    public static void open() {
        if(mongoClient == null || datastore == null) {
            String connectString = LGProperties.get("database.mongo.hostname") + ":" + LGProperties.get("database.mongo.port");
            log.info("Mongo connection :" + connectString);
            mongoClient = new MongoClient(connectString);
            mongoClient.setWriteConcern(WriteConcern.JOURNALED);

            String databaseName = LGProperties.get("database.mongo.databasename");
            if (databaseName == null) {
                databaseName = "lemongrenade_develop";
                log.error("Invalid database name is in properties file. Defaulting to " + databaseName);
            }

            datastore = morphia.createDatastore(mongoClient, databaseName);
            //this.datastore.setDefaultWriteConcern(WriteConcern.ACKNOWLEDGED);
            datastore.setDefaultWriteConcern(WriteConcern.JOURNALED);
        }
    }

    //Open the connection when declaring a new MorphiaService
    public MorphiaService(){
        open();
    }

    public Morphia getMorphia() {return morphia;}

    public void setMorphia(Morphia morphia) {this.morphia = morphia;}

    public Datastore getDatastore() {return datastore;}

    public void setDatastore(Datastore datastore) {this.datastore = datastore;}

}
