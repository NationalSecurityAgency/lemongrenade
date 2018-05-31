package lemongrenade.core.handlers.LGPayload;

import lemongrenade.core.handlers.Handler;
import lemongrenade.core.models.LGPayload;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

//Adapter handler which copies values to a namespace for all  nodes and edges. Excludes type/value by default.
public class NameSpacer implements Handler<LGPayload> {
    private static final Logger log = LoggerFactory.getLogger(NameSpacer.class);
    String namespace = null;
    Set<String> ignore = new HashSet();//Ignore items will be duplicated in the namespace and at the high level. Ignored for removal.

    public String getNamespace() {return namespace;}

    public void setNamespace(String namespace) {this.namespace = namespace;}

    public NameSpacer(String namespace) {
        this.namespace = namespace;
        ignore.add(namespace);
        ignore.add("type");
        ignore.add("value");
        ignore.add("cost");
        ignore.add("LG:METADATA");
    }

    //Removes all items from ignore set
    public void clearIgnore() {
        ignore.clear();
    }

    //Add new items to ignore removing when namespacing
    public void addIgnore(String item) {
        ignore.add(item);
    }

    @Override public void handle(LGPayload payload) {

        //Gather 4 node sets. 1 nodes and 3 "edges" nodes.
        List<JSONObject> nodes = payload.getResponseNodesAndEdges();

        //Iterate through every node
        for(int i = 0; i < nodes.size(); i++) {
            JSONObject node = nodes.get(i);
            JSONObject nameObject = new JSONObject();//object to hold all keys under given namespace
            if(node.has(namespace)) { //If node already has namespace, add to it if JSONObject, else add it in
                try {
                    nameObject = node.getJSONObject(namespace);//add to the old namespace to prevent nesting the same namespace
                }
                catch(JSONException e) {}//couldn't parse JSON, just add this as a usual item
            }

            //Iterate over every key in the node
            Iterator keyIterator = node.keySet().iterator();
            HashSet removeKeys = new HashSet();
            while(keyIterator.hasNext()) {
                String key = keyIterator.next().toString();
                Object value = node.get(key);
                nameObject.put(key, value);//add key to namespace
                if(!ignore.contains(key)) {
                    removeKeys.add(key);//add key to high-level remove list
                }
            }

            //Remove all all high-level keys that aren't on the ignore list
            Iterator iterator = removeKeys.iterator();
            while(iterator.hasNext()) {
                String key = iterator.next().toString();
                node.remove(key);
            }

            node.put(namespace, nameObject);//alter current node before moving to next one
            log.debug("Adding namespace:" + namespace + " to node type:" + node.get("type").toString() + " value:" + node.get("value").toString());
        }
    }

}
