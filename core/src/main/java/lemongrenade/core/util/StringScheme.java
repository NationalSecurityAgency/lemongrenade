package lemongrenade.core.util;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import lemongrenade.core.models.LGPayload;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * Deserializes a message coming off of RabbitMQ to head to the lemongrenade.core.coordinator or adapter
 */
public class StringScheme implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        LGPayload payload = LGPayload.deserialize(byteBuffer.array());
        return new Values(payload.getJobId(), payload);
    }

    public Fields getOutputFields() {
        return new Fields(LGConstants.LG_JOB_ID, LGConstants.LG_PAYLOAD);
    }
}
