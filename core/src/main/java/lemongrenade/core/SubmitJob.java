package lemongrenade.core;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lemongrenade.core.coordinator.CoordinatorTopology;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.database.lemongraph.LemonGraph;
import lemongrenade.core.models.LGCommand;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Submits a job to command queue LGConstants.LEMONGRENADE_COORDINATORCMD
 * TODO: Put sample job config file here
 *
 * Or you can call this from other code:
 *      job = buildJobSubmitObject(alist, payload);
 *      sendNewJobToCommandController(JSONObject job)
 *
 * For testing code, where you just want to submit a job and want to use the 'dummy' payload:
 *      job = buildJobSubmitObject(alist, jobId);
 *      sendNewJobToCommandController(JSONObject job)
 *
 */

public class SubmitJob {
    private final static Logger log = LoggerFactory.getLogger(SubmitJob.class);
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private boolean connectedToQueue = false;
    private LemonGraph lg;
    private JobManager jobManager;
    public SubmitJob() {
        lg = new LemonGraph();
        jobManager = new JobManager();
        factory = new ConnectionFactory();
        factory.setHost(LGProperties.get("rabbit.hostname"));
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATORCMD, true, false, false, CoordinatorTopology.queueArgs);
            connectedToQueue = true;
        }
        catch (java.io.IOException e) {
            log.error("Unable to connect to rabbitmq "+LGConstants.LEMONGRENADE_COORDINATORCMD+ " "+e.getMessage());
            connectedToQueue = false;
        }
        catch (TimeoutException e) {
            log.error("Timeout connecting to rabbitmq "+LGConstants.LEMONGRENADE_COORDINATORCMD+ " "+e.getMessage());
            connectedToQueue = false;
        }
    }

    /** */
    public void closeConnections() throws Exception {
        channel.close();
        connection.close();
    }

    /** */
    public void sendAddToJobToCommandController(String jobId, ArrayList<String> approvedAdapters, LGPayload seedPayload)
    throws Exception
    {
        int ttl = 0;
        if (seedPayload.getJobConfig().has("ttl")) {
            ttl = seedPayload.getJobConfig().getInt("ttl");
        }

        seedPayload.setJobId(jobId);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_ADD, jobId, ttl, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                , cmd.toByteArray());
    }

    /** See api/Job.java for complete description
     */
    public void sendExecutOnNodesToJobToCommandController(String jobId, LGPayload seedPayload)
            throws Exception
    {
        int ttl = 0;
        if (seedPayload.getJobConfig().has("ttl")) {
            ttl = seedPayload.getJobConfig().getInt("ttl");
        }

        seedPayload.setJobId(jobId);
        ArrayList<String> approvedAdapters = new ArrayList<>();
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_EXECUTE_ON_NODES, jobId, ttl, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                , cmd.toByteArray());
    }


    /**
     * See api/Job.java for complete description
     * @param jobId
     * @param reason
     * @return none
     * */
    public void sendReset(String jobId, String reason) throws Exception {
        if (!connectedToQueue) {
            log.error("Unable to send to Queue, connection not open.");
            throw new Exception("Unable to send to Queue, connection not open.");
        }

        ArrayList approvedAdapters = new ArrayList<String>();
        LGPayload cmdPayload = new LGPayload(jobId);
        JSONObject jobConfig = new JSONObject();
        jobConfig.put(LGConstants.LG_RESET_REASON,reason);
        cmdPayload.setJobConfig(jobConfig);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_RESET, jobId, 100, 255, approvedAdapters, cmdPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_COMMAND).build()
                , cmd.toByteArray());
    }

    /**
     * @return JobId String
     * */
    public String sendNewJobToCommandController( ArrayList<String> approvedAdapters, LGPayload seedPayload)
            throws Exception
    {
        int ttl = 0;
        if (seedPayload.getJobConfig().has("ttl")) {
            ttl = seedPayload.getJobConfig().getInt("ttl");
        }
        // TODO: Priority?
        String jobId = "";
        String graphStore = LGProperties.get("coordinator.graphstore");
        if ((graphStore != null) && (graphStore.equalsIgnoreCase("lemongraph"))) {
            try {
                jobId = lg.createGraph(seedPayload.getJobConfig());
            } catch(Exception e) {
                throw e;
            }
        } else {
            // Internal database doesn't care as much
            jobId = UUID.randomUUID().toString();
        }

        seedPayload.setJobId(jobId);
        // TODO: Check to make sure job doesn't already exist?


        // Create Job in Lemongraph so queries after this call will get a value
        seedPayload.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_COMMAND);
        LGJob job = new LGJob(jobId, approvedAdapters, seedPayload.getJobConfig());
        jobManager.addJob(job);


        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_NEW, jobId, ttl, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                , cmd.toByteArray());
        return jobId;
    }

    /** */
    private String readFile(String filename)
    throws Exception {
        String result;
        try {
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

    /** */
    private JSONObject parseJobFile(String filename) {
        System.out.println("Parsing job file :"+filename);
        JSONObject jsonObject = new JSONObject();
        try {
            String jsonData = readFile(filename);
            jsonObject = new JSONObject(jsonData);
        }
        catch (Exception ex) {
            System.out.println("Error trying to find job json file :"+filename+" "+ex.getMessage());
            System.exit(-1);
        }
        return jsonObject;
    }

    /** */
    public  JSONObject readJobFromJsonFile(String jobFile) {
        File f = new File(jobFile);
        if(f.exists()) {
            System.out.println("Unable to find default job file "+jobFile+"... Exiting");
            System.exit(0);
        }
        JSONObject config = parseJobFile(jobFile);
        JSONObject job = config.getJSONObject("job");
        return job;
    }

    /**
     * Builds a job with dummy payload based off job_id. (for testing purposes mostly)
     */
    public static JSONObject buildJobSubmitObject(List approvedAdapters, String jobId) {
        JSONObject node = new JSONObject();
        node.put("status","new");
        node.put("type","id");
        node.put("value",UUID.randomUUID().toString());
        return buildJobSubmitObject(approvedAdapters, jobId, node);
    }

    public static JSONObject buildJobSubmitObject(List approvedAdapters, String jobId, JSONObject node) {
        LGPayload lgp = new LGPayload(jobId);
        if(!node.has("type")) //add "type" if not present
            node.put("type","id");
        if(!node.has("value")) //add value if not present
            node.put("value",UUID.randomUUID().toString());
        lgp.addResponseNode(node);
        return buildJobSubmitObject(approvedAdapters, lgp);
    }

    /**
     * Builds job with given payload (jobid is in payload)
     */
    public static JSONObject buildJobSubmitObject(List approvedAdapters, LGPayload lgp) {
        String jobId = lgp.getJobId();
        JSONObject job = new JSONObject();
        JSONObject pj = new JSONObject(lgp.toJsonString());
        job.put("payload", pj);
        job.put("job_id",jobId);
        JSONArray ja = new JSONArray(approvedAdapters);
        job.put("approvedadapters", ja);
        return(job);
    }

    /** */
    public void printUsage() {
        System.out.println("");
        System.out.println(" SubmitJob:");
        System.out.println("  --help            Print this help message.");
        System.out.println("  --jobfile <file>  Json job file to read and submit.");
        System.out.println("  --testjob  Will submit a test job with fake payload.");
        System.out.println("  --testjobadapters [adapter1 adapter2 adapter3]  Will submit a test job with fake payload.");
        System.out.println("");
        System.out.println("");
        System.out.println(" Examples:");
        System.out.println("     java -jar lemongrenade.jar SubmitJob --jobfile job.json");
        System.out.println("     java -jar lemongrenade.jar SubmitJob --testjob");
        System.out.println("     java -jar lemongrenade.jar SubmitJob --testjobadapters adapter1 adapter2 adapter3");
        System.out.println("");
        System.exit(0);
    }

    /**
     * Command line submit tool
     *
     *  --jobfile <jobfile>  Submits a job via a json file
     *  --testjob             Submits a test job
     *  --testjobadapters adapter1 adapter2  Submits a test job to specified adapters
     *
     */
    public static void main(String[] args) throws Exception {
        Options options;
        options = new Options();

        SubmitJob sj = new SubmitJob();

        options.addOption("h","help", false, "Show help.");
        options.addOption("j","jobfile",true, "JSON job file to read");
        options.addOption("t","testjob",false, "Submit a basic test job.");
        Option ad = new Option("a","testjobadapters",true, "Submit a basic test job with adapter list.");
        ad.setArgs(10);  // allow up to 10 adapters on command line
        options.addOption(ad);
        ArrayList<String> alist;

        String jobId = UUID.randomUUID().toString();
        alist = new ArrayList<String>();
        alist.add("HelloWorld");
        alist.add("PlusBang");
        alist.add("HelloWorldPython");
        //alist.add("LongRunningTestAdapter");
        //alist.add("LongRunningPython");

        JSONObject jobConfig = new JSONObject();
        jobConfig.put("job_id",jobId);
        jobConfig.put("ttl",30); // 30 secs
        jobConfig.put("depth",3);
        jobConfig.put("description","Submit Job tester "+jobId);
        LGPayload lgp = new LGPayload(jobConfig);
        JSONObject node = new JSONObject();
        node.put("status","new");
        node.put("type","id");
        node.put("value",UUID.randomUUID().toString());
        lgp.addResponseNode(node);

        sj.sendNewJobToCommandController( alist, lgp);
        // Close up queue connections
        sj.closeConnections();
    }
}
