package lemongrenade.core.templates;

import lemongrenade.core.util.LGProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

import static lemongrenade.core.util.ReadFile.readFile;

public abstract class LGAdapter implements Serializable{
    final public static String DEFAULT_CONFIG_FILENAME = "LGAdapter.json";
    final public static String DEFAULT_CERTS_FILENAME = "certs.json";
    final public static Config DEFAULT_CONFIG = setDefaultConfig(DEFAULT_CONFIG_FILENAME, DEFAULT_CERTS_FILENAME);
    public final static int ADAPTER_STATE_REGISTERING    = 1;
    public final static int ADAPTER_STATE_ONLINE         = 2;
    public final static int ADAPTER_STATE_HEARTBEAT_FAIL = 3;
    public final static int ADAPTER_STATE_OFFLINE        = 4;
    public UUID id;
    private Config config; //adapter config
    private transient static final Logger log = LoggerFactory.getLogger(LGJavaAdapter.class);
    private long startTime = System.currentTimeMillis();

    public LGAdapter(String id) {
        this.config = setDefaultConfig(DEFAULT_CONFIG_FILENAME, DEFAULT_CERTS_FILENAME);
        this.config.put("name", this.getAdapterName());
        this.id = UUID.fromString(id);
    }

    public LGAdapter(String id, String config_filename) { //constructor reading an input config file
        this.config = readConfig(config_filename);
        this.id = UUID.fromString(id);
    }

    public UUID getAdapterIdByUUID() {
        return id;
    }
    public String getAdapterId() {
        return id.toString();
    }
    public String getQueueName() {
        return getAdapterName() +"-"+getAdapterId();
    }

    public void submitTopology(String[] args) throws Exception {
        if(args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], this.getConfig(), this.getTopology());
        }
    }

    public void runLocal() throws Exception {
        LocalCluster cluster = new LocalCluster();
        if(getConfig() == null)
            setConfig();//uses the default config
        cluster.submitTopology("lemongrenade-test", getConfig(), getTopology());
        Utils.sleep(90000000);
        cluster.killTopology("lemongrenade-test");
        cluster.shutdown();
    }

    public Config getConfig() {
        if(this.config == null) {
            try {
                this.setConfig();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this.config;
    }

    public static Config setDefaultConfig(String config_filename, String certs_filename) {
        Config config = readConfig(config_filename);
        Config certs = readConfig(certs_filename);
        config.putAll(certs);
        return config;
    }

    public void setConfig() throws IOException {
        this.config = DEFAULT_CONFIG;
    }

    // Sets this.config based on
    public void setConfig(String file) throws IOException {
        this.config = readConfig(file);
    }

    // Sets this.config based on
    public static Config readConfig(String file) {
        String content = null;
        try {
            content = readFile(file);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);//must have valid config file
        }
        JSONObject configJSON = new JSONObject(content);
        Config conf = new Config();
        Iterator<?> keyRing = configJSON.keys();
        while(keyRing.hasNext()) {
            String key = (String)keyRing.next();
            Object value = configJSON.get(key);
            //System.out.println("Putting key:"+key+" value:"+value);
            conf.put(key, value);
        }
        return conf;
    }

    public interface LGCallback {
        void emit(JSONObject resp);
        void fail(Exception ex);
    }

    // Default, can be overridden
    public int getParallelismHint(){
        return LGProperties.getInteger("adapters.threads", 6);
    }

    public abstract StormTopology getTopology();
    public abstract String getAdapterName();
    public abstract HashMap<String, String> getRequiredAttributes(); //Implement for attributes required to fire a job on the adapter when NOT using LemonGraph. LIMIT 1 Attribute!!!
    public abstract String getAdapterQuery(); //Implement for attributes required to fire a job on the adapter using LemonGraph
    public int getAdapterDepth() { return 0; };  // Allow adapter writer to assign a depth to the LG query can be overridden
}
