package lemongrenade.core.util;

import lemongrenade.core.models.LGCommand;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * Deserializes a message coming off of RabbitMQ to head to the lemongrenade.coordinator or adapter
 */
public class StringSchemeLGCommand implements Scheme{

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        LGCommand cmd = LGCommand.deserialize(byteBuffer.array());
        return new Values(cmd.getJobId(), cmd);
    }

    public Fields getOutputFields() {
        return new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_COMMAND);
    }
}
