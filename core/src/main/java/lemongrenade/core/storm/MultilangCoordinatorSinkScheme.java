package lemongrenade.core.storm;

import org.apache.storm.tuple.Tuple;
import io.latent.storm.rabbitmq.TupleToMessage;
import lemongrenade.core.models.LGPayload;
import lemongrenade.core.util.LGConstants;
import org.json.JSONObject;

public class MultilangCoordinatorSinkScheme extends TupleToMessage {

    @Override
    protected byte[] extractBody(Tuple tuple) {
        LGPayload payload = LGPayload.fromJson(new JSONObject(tuple.getStringByField(LGConstants.LG_PAYLOAD)));
        return payload.toByteArray();
    }

    @Override
    protected String determineExchangeName(Tuple tuple) {
        return LGConstants.LEMONGRENADE_COORDINATOR;
    }
}
