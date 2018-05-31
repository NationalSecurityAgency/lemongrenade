package handlers.adapters;

import lemongrenade.core.handlers.HandlerLoader;
import lemongrenade.core.handlers.LGPayload.EdgeRemover;
import lemongrenade.core.handlers.LGPayload.NameSpacer;
import lemongrenade.core.models.LGPayload;
import org.apache.storm.utils.Utils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/*
* In an actual adapter, create 2 AdapterHandlerLoaders, 1 ingress, 1 egress.
* Load necessary AdapterHandlers for specific adapter
* Have loader performHandling on ingress/egress at start and end of processing.
* SC template can add some standard handlers. Adapters can then add
* more-specific handlers on init.
 */

public class AdapterHandlerTest {
    private static final Logger log = LoggerFactory.getLogger(AdapterHandlerTest.class);

    @Test public void testNamespacer() {
        LGPayload payload = new LGPayload();
        JSONObject node = new JSONObject()
                .put("type", "type")
                .put("value", "value")
                .put("other", "other")
                ;
        payload.addResponseNode(node);
        NameSpacer nameSpacer = new NameSpacer("NS:TEST");
        nameSpacer.handle(payload);
        List<JSONObject> nodes = payload.getResponseNodes();
        Iterator iterator = nodes.iterator();
        while(iterator.hasNext()) {
            node = (JSONObject) iterator.next();
            assert node.has("type");
            assert node.has("value");
            assert node.has("NS:TEST");
            assert node.has("missingValue") == false;
            JSONObject test = node.getJSONObject("NS:TEST");
            assert test.has("type") == true;
            assert test.has("value") == true;
        }
    }

    @Test public void testLoadHandlers() throws Exception {
        HandlerLoader<LGPayload> loader = new HandlerLoader<>();
        loader.loadHandlers("LGAdapter.example.json", "handler_data.LGPayload.EGRESS");
        loader.addHandler(new NameSpacer("TEST"));
        assert loader.getHandlers().size() == 3;
        assert loader.getHandlers().get(0) instanceof NameSpacer;
        assert loader.getHandlers().get(1) instanceof EdgeRemover;
        assert loader.getHandlers().get(2) instanceof NameSpacer;
        assert ((NameSpacer)loader.getHandlers().get(0)).getNamespace().equals("TEST");
        LGPayload payload = new LGPayload(Utils.uuid());
        JSONObject node = new JSONObject()
                .put("type", "type")
                .put("value","value")
                .put("StringKey", "StringVal")
                .put("array", new JSONArray().put("item"))
                .put("object", new JSONObject().put("key", "value"))
                ;
        payload.addResponseNode(node);
        loader.performHandling(payload);
        assert payload.getRequestNodes().size() == 0;
        assert payload.getResponseEdges().size() == 0;
        assert payload.getRequestEdges().size() == 0;
        assert payload.getResponseNodes().size() == 1;
        node = payload.getResponseNodes().get(0);
        assert node.has("TEST");
        JSONObject test = node.getJSONObject("TEST");
        assert test.has("array");
        assert test.has("StringKey");
        assert test.has("object");
        assert !node.has("array");
        assert !node.has("StringKey");
        assert node.has("type");
        assert node.has("value");
        assert !node.has("object");
        log.info("done");
    }

    @Test public void testUnloadHandlers() throws Exception {
        String nameSpacer = NameSpacer.class.getName();
        String edgeRemover = EdgeRemover.class.getName();
        HandlerLoader<LGPayload> loader = new HandlerLoader<>();
        loader.loadHandlers("LGAdapter.example.json", "handler_data.LGPayload.EGRESS");
        loader.addHandler(new NameSpacer("TEST"));
        assert loader.getHandlers().size() == 3;
        Set<String> ignores = new HashSet<>();
        ignores.add(nameSpacer);
        loader.unloadHandlers(ignores);
        assert loader.getHandlers().size() == 1;
        String className = loader.getHandlers().get(0).getClass().getName();
        assert className.equals(edgeRemover);
        ignores.add(edgeRemover);
        loader.unloadHandlers(ignores);
        assert loader.getHandlers().size() == 0;

    }

}
