package lemongrenade.core.handlers.LGPayload;

import lemongrenade.core.handlers.Handler;
import lemongrenade.core.models.LGPayload;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//Handler that removes all edges from an LGPayload
public class EdgeRemover implements Handler<LGPayload> {
    private static final Logger log = LoggerFactory.getLogger(EdgeRemover.class);

    @Override public void handle(LGPayload payload) {
        log.info("EdgeRemover running.");
        JSONObject requests = payload.getRequests();
        JSONObject responses = payload.getResponses();
        List nodeContainers = new ArrayList<JSONObject>();
        nodeContainers.add(responses);
        nodeContainers.add(requests);
        Iterator iterator = nodeContainers.iterator();
        while(iterator.hasNext()) {
            JSONObject container = (JSONObject) iterator.next();
            container.put("edges", new JSONArray());
        }
    }
}
