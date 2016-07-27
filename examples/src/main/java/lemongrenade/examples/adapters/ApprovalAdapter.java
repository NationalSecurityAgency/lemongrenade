package lemongrenade.examples.adapters;

import lemongrenade.core.models.LGPayload;
import lemongrenade.core.templates.LGJavaAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

//Takes value for "task_adapter" key and puts it as a key with value "approved"
public class ApprovalAdapter extends LGJavaAdapter {
    private static final Logger log = LoggerFactory.getLogger(ApprovalAdapter.class);

    public ApprovalAdapter(String id) {
        super(id);
    }

    @Override
    public void process(LGPayload input, LGCallback callback) {
        input.getRequestNodes().forEach(request -> {
            Object value = request.get("task_adapter");
            request.put(value.toString(), "approved");
            input.addResponseNode(request);
        });
        callback.emit(input);
    }

    @Override
    public String getAdapterName() {
        return "Approval";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> requirements = new HashMap<String, String>();
        requirements.put("task_adapter", ".*");
        return requirements;
    }

    @Override
    public String getAdapterQuery() {
        return "n(task_adapter~/.*/i)";
    }

    public static void main(String[] args) throws Exception {
        if (args.length <1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        ApprovalAdapter adapter = new ApprovalAdapter(args[1]);
        adapter.submitTopology(args);
    }

}
