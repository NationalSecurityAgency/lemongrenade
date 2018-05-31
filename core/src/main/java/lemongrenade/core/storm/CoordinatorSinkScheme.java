package lemongrenade.core.storm;

import org.apache.storm.tuple.Tuple;
import lemongrenade.core.util.LGConstants;

public class CoordinatorSinkScheme extends AdapterSinkScheme {

    @Override
    protected String determineExchangeName(Tuple tuple) {
        return LGConstants.LEMONGRENADE_COORDINATOR;
    }

    @Override
    protected String determineRoutingKey(Tuple tuple) {
        return LGConstants.LEMONGRENADE_COORDINATOR;
    }
}
