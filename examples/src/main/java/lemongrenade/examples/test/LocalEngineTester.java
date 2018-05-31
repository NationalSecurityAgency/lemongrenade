package lemongrenade.examples.test;

import lemongrenade.core.templates.LGAdapter;
import lemongrenade.core.util.LGProperties;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.TestJob;
import org.apache.storm.utils.Utils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;

/**
 *
 * Reads the topology configuration file and starts the adapters
 * It takes a command line topology file name as the first argument
 * and will attempt to load this file as a class then a local file if unsuccessful.
 *
 * See /src/java/resources/lemongrenade-topology.json for example
 *
 */
public class LocalEngineTester {
    private final static Logger log = LoggerFactory.getLogger(LocalEngineTester.class);

    /**
     * Checks for the bare essentials configuration items
     * @param adapterObj
     * @return
     */
    private static boolean checkAdapterObject(JSONObject adapterObj) {
        boolean result = true;

        if (!adapterObj.has("id")) {
            result = false;
            System.out.println("Adapter missing id");
        }
        if (!adapterObj.has("class")) {
            result = false;
            System.out.println("Adapter missing class");
        }
        if (!adapterObj.has("enabled")) {
            result = false;
            System.out.println("Adapter missing enabled setting");
        }
        return result;
    }

    /**
     * Start All adapters in adapters list
     */
    public static void startAllAdapters(JSONArray adapters) {
        System.out.println("Starting Adapters defined in topology configuration file.");

        // ---------------------------------------------------------
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(1);
        mkClusterParam.setPortsPerSupervisor(32); //Hard limit on the number of topologies that can run at once

        ArrayList<LGAdapter> startAdapters = new ArrayList<>();

        for (int i = 0; i < adapters.length(); i++) {
            JSONObject adapter = adapters.getJSONObject(i);
            if (!checkAdapterObject(adapter)) {
                System.out.println("Skipping adapter definition missing configuration information " + adapter.toString());
                continue;
            }

            try {
                // Use fancy reflection to find the adapter class and start it
                String id = adapter.getString("id");
                boolean enabled = adapter.getBoolean("enabled");
                String objectName = adapter.getString("class");
                if (!enabled) {
                    System.out.println("Skipping disabled Adapter " + id + " " + objectName);
                } else {
                    System.out.println("Starting adapter "+objectName);
                    Class cls = Class.forName(objectName);
                    Constructor<?> cons = cls.getConstructor(String.class);
                    Object adapterObject = cons.newInstance(id);
                    java.lang.reflect.Method getAdapterName = adapterObject.getClass().getMethod("getAdapterName");
                    System.out.println(" Starting Adapter:" + getAdapterName.invoke(adapterObject));
                    LGAdapter tmp = (LGAdapter) adapterObject;
                    startAdapters.add(tmp);
                }
            } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException | InstantiationException e) {
                System.out.println(e.getMessage());
            } catch (ClassNotFoundException e) {
                System.out.println("ERROR: Adapter Class not found");
                System.out.println(e.getMessage());
            }
        }


        /**
         * when testing your topology, you need a <code>LocalCluster</code> to run your topologies, you need
         * to create it, after using it, you need to stop it. Using <code>Testing.withLocalCluster</code> you
         * don't need to do any of this, just use the <code>cluster</code> provided through the param of
         * <code>TestJob.run</code>.
         */
        Testing.withLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) {
                try {
                    for (LGAdapter adapter : startAdapters) {
                        cluster.submitTopology(adapter.getAdapterName(), adapter.getConfig(), adapter.getTopology());
                    }
                    while(true) { //Don't time-out this thread. It kills the launched topologies
                        Thread.sleep(300000); // ~18 seconds to boot up on my machine
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }//end of startAllAdapters


    /**
     *
     * @param filename
     * @return
     * @throws Exception
     */
    private static String readFile(String filename) throws Exception {
        String result = "";
        try {
            System.out.println("Looking for "+filename+" in "+System.getProperty("user.dir"));
            BufferedReader br = new BufferedReader(new FileReader(filename));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            throw e;
        }
        return result;
    }

    private static String readFile(InputStream inputStream) throws Exception {
        String result = "";
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            throw e;
        }
        return result;
    }

    /**
     *
     * @param filename
     * @return
     */
    private static JSONObject parseTopologyFile(String filename) {
        System.out.println("Parsing topology file :"+filename);
        JSONObject jsonObject = new JSONObject();
        InputStream file_stream = null;
        try {
            file_stream = LGProperties.getStream(filename);
            String jsonData = readFile(file_stream);
            jsonObject = new JSONObject(jsonData);
        } catch (Exception ex) {
            System.out.println("Error trying to find file :" + filename);
            ex.printStackTrace();
            System.exit(-1);
        } finally {
            if(file_stream != null) {
                try {
                    file_stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return jsonObject;
    }

    /**
     * Looks for arg lemongrande topology file. If not, looks in the path
     * for a file named 'lemongrenade-topology.json'
     *
     * @param args
     * @return JSONObject configuration file
     */
    private static JSONObject readLemongrenadeTopologyFile(String[] args) {
        String topologyConfigFile;
        if (args.length <= 0) {
            topologyConfigFile = "default-topology.json";
        } else {
            topologyConfigFile = args[0];
        }
        JSONObject config = parseTopologyFile(topologyConfigFile);
        JSONObject lemongrenadeConfig = config.getJSONObject("lemongrenade-topology");
        return lemongrenadeConfig;
    }

    private static boolean checkResource(String resource) {//returns True if Java resource is present
        try {
            URL u = LocalEngineTester.class.getClassLoader().getResource(resource);
            if(u != null)
                return true;//resource found, return true
            return false; //resource NOT found, return false
        }
        catch(Exception ex) {
            return false;
        }
    }

    /**
     * Main Engine loop
     *    Reads the lemongrenade topology configuration file and starts all
     *    configured adapters. Then it starts the coordinator processes, which
     *    will eventually be part of the topology file as well.
     *
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args)
    throws Exception
    {
        // Read Topology Configuration file
        JSONObject lemongrenade = readLemongrenadeTopologyFile(args);

        // Start all the adapters from the config file
        JSONArray adapters = lemongrenade.getJSONArray("adapters");
        startAllAdapters(adapters);

        // Enter main loop - not sure what we will do here
        Utils.sleep(12000);
        while(true) {
            log.info("Engine Loop");
            Utils.sleep(3000);
        }
    }
}