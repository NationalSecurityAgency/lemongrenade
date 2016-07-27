package lemongrenade.examples.adapters;

import lemongrenade.core.storm.LGShellBolt;
import lemongrenade.core.templates.LGMultilangAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;

public class HelloWorldPython3Adapter extends LGMultilangAdapter {

    public HelloWorldPython3Adapter(String id) {
        super(id);
    }
    private static final Logger log = LoggerFactory.getLogger(HelloWorldPython3Adapter.class);

    @Override
    public LGShellBolt.SupportedLanguages getAdapterType() {
        return LGShellBolt.SupportedLanguages.PYTHON3;
    }

    @Override
    public String getScriptName() {
        return "resources/python3/HelloWorld.py";
    }

    @Override
    public String getAdapterName() {
        return "HelloWorldPython3";
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
        if (args.length < 1) {
            System.out.println("ERROR: Missing adapter ID");
            System.exit(-1);
        }
        HelloWorldPython3Adapter adapter = new HelloWorldPython3Adapter(args[1]);
        adapter.submitTopology(args);
    }
}
