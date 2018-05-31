package lemongrenade.core.util;

public class LGConstants {

    /* Common strings used throughout LG code */
    public final static String LGADAPTERS             = "lemongrenade-adapters";
    public final static String LGADAPTERSDATA         = "lemongrenade-adaptersdata";
    public final static String LEMONGRENADE_COORDINATOR    = "lemongrenade-core-coordinator";
    public final static String LEMONGRENADE_COORDINATORCMD = "lemongrenade-core-coordinatorcommand";
    public final static String DEADLETTER_QUEUE            = "DeadLetter";
    public final static String LG_COMMAND = "lg_command";
    public final static String LG_PAYLOAD = "lg_payload";
    public final static String LG_JOB_ID  = "job_id";

    public final static String LG_INTERNAL_OP   = "lg_internal_op";
    public final static String LG_INTERNAL_DATA = "lg_internal_data";
    public final static String LG_INTERNAL_OP_EXECUTE_ON_ADAPTERS = "postaction";



    public final static int QUEUE_PRIORITY_COMMAND = 10;         // Commands like stop highest priority
    public final static int QUEUE_PRIORITY_ADAPTER_RESPONSE = 5;
    public final static int QUEUE_PRIORITY_NEW_JOB = 1;          // New jobs have lowest priority

    public final static String LG_PAYLOAD_TYPE_COMMAND = "cmd";
    public final static String LG_PAYLOAD_TYPE_ADAPTERRESPONSE = "adapter";
    public final static String LG_PAYLOAD_TYPE_ADAPTERRESPONSE_FAILURE = "adapter-failed";

    public final static String LG_RESET_REASON = "reason";
    public final static String LG_RESET_OVERWRITE = "overwrite";


}
