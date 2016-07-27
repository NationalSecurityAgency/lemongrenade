package lemongrenade.examples.adapters;

import lemongrenade.core.storm.LGShellBolt;
import lemongrenade.core.templates.LGMultilangAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

public class HelloWorldNodeAdapter extends LGMultilangAdapter {
    public HelloWorldNodeAdapter(String id) {
        super(id);
    }
    private static final Logger log = LoggerFactory.getLogger(HelloWorldNodeAdapter.class);

    @Override
    public String getAdapterName() {
        return "HelloWorldNode";
    }

    @Override
    public LGShellBolt.SupportedLanguages getAdapterType() {
        return LGShellBolt.SupportedLanguages.NODEJS;
    }

    @Override
    public String getScriptName() {
        return "node/HelloWorld.js";
    }

    @Override
    public HashMap<String, String> getRequiredAttributes() {
        HashMap<String, String> temp = new HashMap<String, String>();
        temp.put("status", ".*");
        return temp;
    }

    @Override
    public String getAdapterQuery() { //Numbers cannot be regexd
        return "n(status~/.*/i,type~/.*/i,value~/.*/i,status!:number)";//requires a string (any value), any "type", any "value", case insensitive
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 ) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        HelloWorldNodeAdapter adapter = new HelloWorldNodeAdapter(args[1]);
        adapter.submitTopology(args);
    }
}
