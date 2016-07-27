package lemongrenade.core;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lemongrenade.core.coordinator.CoordinatorTopology;
import lemongrenade.core.coordinator.JobManager;
import lemongrenade.core.models.LGCommand;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import lemongrenade.core.util.LGProperties;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class SubmitCancelCommand {

    private final static Logger log = LoggerFactory.getLogger(SubmitCancelCommand.class);
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private boolean connectedToQueue = false;

    public SubmitCancelCommand() {
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
        if (!connectedToQueue) {
            log.error("Unable to send to Queue, connection not open.");
            throw new Exception("Unable to send to Queue, connection not open.");
        }

        ArrayList approvedAdapters = new ArrayList<String>();
        LGPayload seedPayload = new LGPayload(jobId);
        LGCommand cmd = new LGCommand(LGCommand.COMMAND_TYPE_STOP, jobId, 100, 255, approvedAdapters, seedPayload);
        channel.basicPublish("", LGConstants.LEMONGRENADE_COORDINATORCMD
                , new AMQP.BasicProperties.Builder().priority(LGConstants.QUEUE_PRIORITY_COMMAND).build()
                , cmd.toByteArray());
    }

    /** */
    public void printUsage() {
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
     */
    public static void main(String[] args) throws Exception {
        Options options;
        options = new Options();
        SubmitCancelCommand scc = new SubmitCancelCommand();
        options.addOption("h","help", false, "Show help.");
        options.addOption("j","job",true, "Job ID");
        options.addOption("a","all",false, "Stop All Jobs");

        CommandLineParser parser = new BasicParser();
        CommandLine  cmd = null;
        //TESTING: scc.cancelAllActiveJobs();
        scc.cancelAllActiveJobs();
        /*
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                scc.printUsage();
            } else if (cmd.hasOption("j")) {
                String jobId = cmd.getOptionValue("j");
                scc.sendCancel(jobId);
            } else {
                log.error("Unable to figure out how you want me to cancel job.");
                scc.printUsage();
            }
        }
        catch (ParseException e) {
            scc.printUsage();
        }
*/
        // Close up queue connections
        scc.closeConnections();
    }
}
