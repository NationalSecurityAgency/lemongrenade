package lemongrenade.core;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lemongrenade.core.coordinator.CoordinatorTopology;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.database.lemongraph.InvalidGraphException;
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
 */

public class SubmitToRabbitMQ {
    private final static Logger log = LoggerFactory.getLogger(SubmitToRabbitMQ.class);
    static private ConnectionFactory factory;
    static private Connection connection;
    static private Channel channel;

    static {
        try {
            factory = new ConnectionFactory();
            factory.setHost(LGProperties.get("rabbit.hostname"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public SubmitToRabbitMQ() {}

    public void openConnections() throws Exception {
        if(connection == null || !connection.isOpen() || channel == null || !channel.isOpen() ) {
            try {
                log.info("Opening RabbitMQ connection and COORDINATORCMD channel.");
                connection = factory.newConnection();
                channel = connection.createChannel();
                channel.queueDeclare(LGConstants.LEMONGRENADE_COORDINATORCMD, true, false, false, CoordinatorTopology.queueArgs);
            } catch (java.io.IOException e) {
                log.error("Unable to connect to rabbitmq " + LGConstants.LEMONGRENADE_COORDINATORCMD + " " + e.getMessage());
            } catch (TimeoutException e) {
                log.error("Timeout connecting to rabbitmq " + LGConstants.LEMONGRENADE_COORDINATORCMD + " " + e.getMessage());
            }
        } else {
            log.debug("Connection and Channel are open.");
        }
    }

    /** Closes RabbitMQ connection, COORDINATORCMD channel, and LemonGraph
     * @throws Exception If the connection cannot be closed
     * */
    public void close() throws Exception {
        closeChannel(channel);
        closeConnection(connection);
        log.info("Closing connection to LemonGraph.");
        LemonGraph.close();
        JobManager.close();
    }

    //Closes COORDINATORCMD channel on RabbitMQ
    public static void closeChannel(Channel channel) throws Exception {
        try {
            if (channel != null && channel.isOpen()) {
                log.info("Closing COORDINATORCMD channel.");
                channel.close();
            }
        }
        catch(com.rabbitmq.client.AlreadyClosedException e) {
            log.info("COORDINATORCMD channel already closed.");
        }
    }

    //Closes connection to RabbitMQ.
    public static void closeConnection(Connection connection) throws Exception {
        try {
            if(connection != null && connection.isOpen()) {
                log.info("Closing RabbitMQ connection.");
                connection.close();
            }
        }
        catch(com.rabbitmq.client.AlreadyClosedException e) {
            log.info("RabbitMQ connection already closed.");
        }
    }

    /**
     * @param jobId LG job ID to send to
     * @param approvedAdapters List of approved adapter names
     * @param seedPayload LGPayload for seeding a new job
     * */
    public void sendAddToJobToCommandController(String jobId, ArrayList<String> approvedAdapters, LGPayload seedPayload) throws Exception {
        openConnections();
        int ttl = 0;
        if (seedPayload.getJobConfig().has("ttl")) {
            ttl = seedPayload.getJobConfig().getInt("ttl");
        }

        try {
            LemonGraph.getGraph(jobId); //verify the job already exists in LemonGraph
        }
        catch(Exception e) {
            throw new Exception("job:"+jobId+" doesn't exist. First perform a job create. Error:"+e.getMessage());
        }

        seedPayload.setJobId(jobId);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_ADD, jobId, ttl, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                , cmd.toByteArray());
    }

    /**
     * See api/Job.java for complete description
     * @param jobId LG job ID to send to
     * @param approvedAdapters List of approved adapter names
     * @param seedPayload LGPayload to send to Command Controller
     */
    public void sendPostActionCommandController(String jobId, ArrayList<String> approvedAdapters, LGPayload seedPayload)
            throws Exception {
        openConnections();
        int ttl = 0;
        if (seedPayload.getJobConfig().has("ttl")) {
            ttl = seedPayload.getJobConfig().getInt("ttl");
        }

        seedPayload.setJobId(jobId);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_POST_ACTION, jobId, ttl, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                , cmd.toByteArray());
    }

    /**
     * See api/Job.java for complete description
     * @param jobId String job ID
     * @param reason String reason for reset
     * */
    public void sendReset(String jobId, String reason) throws Exception {
        openConnections();
        ArrayList approvedAdapters = new ArrayList<String>();
        LGPayload cmdPayload = new LGPayload(jobId);
        JSONObject jobConfig = new JSONObject();
        jobConfig.put(LGConstants.LG_RESET_REASON, reason);
        cmdPayload.setJobConfig(jobConfig);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_RESET, jobId, 100, 255, approvedAdapters, cmdPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_COMMAND).build()
                , cmd.toByteArray());
    }

    /**
     * See api/Job.java for complete description
     * @param jobId String for job ID
     * @param taskId String for task ID
     * @throws Exception when failing to send retry.
     */
    public void sendRetry(String jobId, String taskId) throws Exception {
        openConnections();
        ArrayList approvedAdapters = new ArrayList<String>();
        LGPayload cmdPayload = new LGPayload(jobId);
        JSONObject jobConfig = new JSONObject();
        JSONArray tasks = new JSONArray();  // So in the future we can support a list of taskIds
        if (!taskId.equals("")) {
            tasks.put(taskId);
        }
        jobConfig.put("tasks", tasks);
        cmdPayload.setJobConfig(jobConfig);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_RETRY_FAILED_TASKS, jobId, 100, 255, approvedAdapters, cmdPayload);
        channel.basicPublish(
                "" //exchange
                , LGConstants.LEMONGRENADE_COORDINATORCMD //routing key
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_COMMAND).build()
                , cmd.toByteArray());
    }

    public LGJob sendNewJobToCommandController(ArrayList<String> approvedAdapters, LGPayload seedPayload) throws Exception {
        openConnections();
        int ttl = 0;
        if (seedPayload.getJobConfig().has("ttl")) {
            ttl = seedPayload.getJobConfig().getInt("ttl");
        }
        String jobId = seedPayload.getJobId();
        if(jobId.length() == LGJob.JOB_ID_LENGTH) { //a valid job ID was sent in
            try {
                JSONObject job = LemonGraph.getGraph(jobId); //check that a job for given ID doesn't already exist
                if (job.get("id").equals(jobId)) {
                    throw new Exception("jobId:" + jobId + " already exists.");
                }
            }
            catch(InvalidGraphException e) {
                String msg = e.getMessage();
                JSONObject ret = new JSONObject(msg);
                if(!(ret.has("code") && ret.getInt("code") == LemonGraph.NOT_FOUND)) {
                    throw e;
                }
            }
        }
        String graphStore = LGProperties.get("coordinator.graphstore");
        if ((graphStore != null) && (graphStore.equalsIgnoreCase("lemongraph"))) {
            try {
                jobId = LemonGraph.createGraph(jobId, seedPayload.getJobConfig()); //Adds new job to LEMONGRAPH and gets jobId
            } catch(Exception e) {
                throw e;
            }
        } else {
            // Internal database doesn't care as much
            jobId = UUID.randomUUID().toString();
        }
        seedPayload.setJobId(jobId);
        // Create Job in Lemongraph so queries after this call will get a value
        seedPayload.setPayloadType(LGConstants.LG_PAYLOAD_TYPE_COMMAND);
        LGJob job = new LGJob(jobId, approvedAdapters, seedPayload.getJobConfig());
        JobManager.addJob(job); //adds new job to MongoDB


        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_NEW, jobId, ttl, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_NEW_JOB).build()
                , cmd.toByteArray());
        return job;
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
        node.put("type", "id");
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
    public void cancelAllActiveJobs() throws Exception {
        JobManager jb = new JobManager();
        List<LGJob> activeJobs = jb.getAllActive();
        for (LGJob j: activeJobs) {
            log.info("Cancelling Job :"+j.getJobId());
            sendCancel(j.getJobId());
        }
    }

    /** */
    public void sendCancel(String jobId) throws Exception {
        openConnections();
        ArrayList approvedAdapters = new ArrayList<String>();
        LGPayload seedPayload = new LGPayload(jobId);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_STOP, jobId, 100, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_COMMAND).build()
                , cmd.toByteArray());
    }

    /** */
    public void printSubmitJobUsage() {
        System.out.println("");
        System.out.println(" SubmitToRabbitMQ:");
        System.out.println("  --help            Print this help message.");
        System.out.println("  --jobfile <file>  Json job file to read and submit.");
        System.out.println("  --testjob  Will submit a test job with fake payload.");
        System.out.println("  --testjobadapters [adapter1 adapter2 adapter3]  Will submit a test job with fake payload.");
        System.out.println("");
        System.out.println("");
        System.out.println(" Examples:");
        System.out.println("     java -jar lemongrenade.jar SubmitToRabbitMQ --jobfile job.json");
        System.out.println("     java -jar lemongrenade.jar SubmitToRabbitMQ --testjob");
        System.out.println("     java -jar lemongrenade.jar SubmitToRabbitMQ --testjobadapters adapter1 adapter2 adapter3");
        System.out.println("");
        System.exit(0);
    }

    /** */
    public void printCancelJobUsage() {
        System.out.println("");
        System.out.println(" Cancel Job:");
        System.out.println("  --help            Print this help message.");
        System.out.println("  --job <jobid>     Job ID to cancel");
        System.out.println("  --all             Kill all active jobs");
        System.out.println("");
        System.out.println(" Examples:");
        System.out.println("     java -jar lemongrenade.jar SubmitCancelCommand --job <jobid>");
        System.out.println("");
        System.exit(0);
    }


    /**
     * Command line submit tool
     * @param args
     * --jobfile |jobfile|  Submits a job via a json file
     * --testjob             Submits a test job
     * --testjobadapters adapter1 adapter2  Submits a test job to specified adapters
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Options options;
        options = new Options();
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
        node.put("status", "new");
        node.put("type", "id");
        node.put("value", UUID.randomUUID().toString());
        lgp.addResponseNode(node);

        SubmitToRabbitMQ submit = new SubmitToRabbitMQ();

        submit.sendNewJobToCommandController(alist, lgp);
        submit.close();// Close up queue connections
    }
}
