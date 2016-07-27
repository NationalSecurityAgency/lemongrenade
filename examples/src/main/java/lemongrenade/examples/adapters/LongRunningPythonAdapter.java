package lemongrenade.examples.adapters;

import lemongrenade.core.storm.LGShellBolt;
import lemongrenade.core.templates.LGMultilangAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

public class LongRunningPythonAdapter extends LGMultilangAdapter {

    public LongRunningPythonAdapter(String id) {
        super(id);
    }
    private static final Logger log = LoggerFactory.getLogger(LongRunningPythonAdapter.class);

    @Override
    public LGShellBolt.SupportedLanguages getAdapterType() {
        return LGShellBolt.SupportedLanguages.PYTHON;
    }

    @Override
    public String getScriptName() {
        return "resources/python/LongRunningTest.py";
    }

    @Override
    public String getAdapterName() {
        return "LongRunningPython";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("hello", ".*\\d+?$");
        return temp;
    }

    @Override
    public String getAdapterQuery() {
        return "n(hello~/world(\\d+)?$/i)";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        LongRunningPythonAdapter adapter = new LongRunningPythonAdapter(args[1]);
        adapter.submitTopology(args);
    }
}
