package lemongrenade.core.storm;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;

public abstract class LGShellBolt extends ShellBolt implements IRichBolt {

    public enum SupportedLanguages {
        PYTHON("python"), NODEJS("node"), PYTHON3("python3");

        private String stormName;

        SupportedLanguages(String stormName){
            this.stormName = stormName;
        }

        public String getStormName(){
            return this.stormName;
        }
    }

    public LGShellBolt(SupportedLanguages service, String scriptName) {
        super(service.getStormName(), scriptName);
    }
}
