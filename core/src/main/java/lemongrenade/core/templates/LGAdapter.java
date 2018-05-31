package lemongrenade.core.templates;

import lemongrenade.core.util.LGProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

import static lemongrenade.core.util.ReadFile.getContent;

public abstract class LGAdapter implements Serializable{
    final public static String DEFAULT_CONFIG_FILENAME = "LGAdapter.json";
    final public static String DEFAULT_CERTS_FILENAME = "certs.json";
    final public static Config DEFAULT_CONFIG;

    //DEFAULT_CONFIG must be set
    static {
        try {
            DEFAULT_CONFIG = setDefaultConfig(DEFAULT_CONFIG_FILENAME, DEFAULT_CERTS_FILENAME);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public UUID id;
    private Config config = null; //adapter config - LGAdapter.config
    private static transient final Logger log = LoggerFactory.getLogger(LGAdapter.class);

    public void setConfig() {
        if(config == null) {
            config = DEFAULT_CONFIG;
            log.info("Creating adapter Config from '"+DEFAULT_CONFIG_FILENAME+"' and '"+DEFAULT_CERTS_FILENAME+"'.");
            String adapterConfig = getAdapterName()+"Adapter.json";
            try {
                Config specific = readConfig(adapterConfig);
                Iterator iterator = specific.keySet().iterator();
                while(iterator.hasNext()) {//add and override any adapter-specific config fields
                    String key = iterator.next().toString();
                    Object val = specific.get(key);
                    config.put(key, val);
                }
                log.info("Adding adapter-specific config items from '"+adapterConfig+"'.");
            } catch (IOException e) {
                log.warn("Couldn't read file '"+adapterConfig+"'. No adapter-specific config items added.");
            }
            config.put("name", this.getAdapterName());//add adapter name to config
        }
    }

    public LGAdapter(String id) {
        setConfig();
        this.id = UUID.fromString(id);
    }

    public LGAdapter(String id, String config_filename) throws IOException { //constructor reading an input config file
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
            StormTopology topology = this.getTopology();
            StormSubmitter.submitTopology(args[0], this.getConfig(), topology);
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
            this.setConfig();
        }
        return this.config;
    }

    public static Config setDefaultConfig(String config_filename, String certs_filename) throws IOException {
        Config config = readConfig(config_filename);
        Config certs = readConfig(certs_filename);
        config.putAll(certs);
        return config;
    }

    public void setConfig(String file) throws IOException {
        this.config = readConfig(file);
    }

    public static Config readConfig(String file) throws IOException {
        InputStream is = LGProperties.getStream(file);//Reads file from ENV-defined location > default location > class resource
        String content = getContent(is);
        JSONObject configJSON = new JSONObject(content);
        Config conf = new Config();
        Iterator<?> keyRing = configJSON.keys();
        while(keyRing.hasNext()) {
            String key = (String)keyRing.next();
            Object value = configJSON.get(key);
            if(value instanceof JSONArray || value instanceof JSONObject) { //stringify JSON to allow serialization
                value = value.toString();
            }
            conf.put(key, value);
        }
        return conf;
    }

    public interface LGCallback {
        void emit(JSONObject resp);
        void fail(Exception ex);
    }

    public int getTaskCount() {
        try {
            if (getConfig().containsKey("adapter.tasks")) {
                Object hint = config.get("adapter.tasks");
                if (hint instanceof String) {
                    return Integer.parseInt(hint.toString());
                }
                return (int) hint;
            }
        }
        catch(Exception e) {}
        return LGProperties.getInteger("adapter.tasks", getParallelismHint());//default to 1 to 1 with Executors if none are present
    }//get the total number of tasks

    //Adapter authors should override this function to return optional adapter-specific information. E.g. description
    public JSONObject getAuxInfo() {
        return new JSONObject();
    }

    // Get the parallelism hint for adapters - This determines the number of executors it will start with
    public int getParallelismHint() {
        try {
            if (getConfig().containsKey("adapter.threads")) {
                Object hint = config.get("adapter.threads");
                if (hint instanceof String) {
                    return Integer.parseInt(hint.toString());
                }
                return (int) hint;
            }
        }
        catch(Exception e) {}

        try {
            Object hint = config.get("topology.parallelism");
            if(hint instanceof String) {
                return Integer.parseInt(hint.toString());
            }
            return (int)hint;
        }
        catch(Exception e) {}

        if(LGProperties.has("adapter.threads")) {
            return LGProperties.getInteger("adapter.threads", 6);
        }
        return LGProperties.getInteger("topology.parallelism", 6);//default to 6 if neither field is present
    }

    public abstract StormTopology getTopology(); //creates the topology to send to storm
    public abstract String getAdapterName(); //each adapter requires a unique name
    public abstract HashMap<String, String> getRequiredAttributes(); //Implement for attributes required to fire a job on the adapter when NOT using LemonGraph. LIMIT 1 Attribute!!!
    public abstract String getAdapterQuery(); //Implement for attributes required to fire a job on the adapter using LemonGraph
    public int getAdapterDepth() { return 0; };  // Allow adapter writer to assign a depth to the LG query can be overridden
}
