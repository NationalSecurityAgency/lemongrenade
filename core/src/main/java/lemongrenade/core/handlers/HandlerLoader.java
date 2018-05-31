package lemongrenade.core.handlers;

import lemongrenade.core.templates.LGAdapter;
import org.apache.storm.Config;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

//Load handlers then perform processing on an input T.
public class HandlerLoader<T> {

    //Takes a filename for JSONObject-formatted file and field to read a JSONArray of handler class data
    public void loadHandlers(String fileName, String field) throws Exception {
        Config conf = LGAdapter.readConfig(fileName);
        if(!conf.containsKey(field)) {
            throw new Exception("Config doesn't contain key:"+field);
        }
        JSONArray handlerDatas = new JSONArray(conf.get(field).toString());

        Iterator iterator = handlerDatas.iterator();
        while(iterator.hasNext()) {
            JSONObject handlerData = (JSONObject) iterator.next();
            String className = handlerData.getString("class");
            Class cls = Class.forName(className);
            Object handlerObject;
            if(handlerData.has("argument")) {
                Constructor<?> cons = cls.getConstructor(String.class);
                handlerObject = cons.newInstance(handlerData.getString("argument"));
            }
            else {
                Constructor<?> cons = cls.getConstructor();
                handlerObject = cons.newInstance();
            }


            Handler<T> tmp = (Handler) handlerObject;
            addHandler(tmp);
        }
    }

    //Iterates over a set of strings and unloads all handlers matched
    public void unloadHandlers(Set<String> ignores) {
        for(int i = handlers.size()-1; i >= 0; i--) {//start at the end, work to beginning
            String className = handlers.get(i).getClass().getName();
            if(ignores.contains(className)) {
                handlers.remove(i);
            }
        }
    }

    private List<Handler<T>> handlers = new ArrayList();

    public List<Handler<T>> getHandlers() {
        return handlers;
    }

    //Add a new handler to list
    public void addHandler(Handler<T> handler) {
        handlers.add(handler);
    }

    //Clear list of handlers
    public void clearHandlers() {
        handlers.clear();
    }

    //Perform handling on input using all loaded handlers
    public void performHandling(T input) throws Exception {
        Iterator iterator = handlers.iterator();
        while(iterator.hasNext()) {
            Handler<T> handler = (Handler<T>) iterator.next();
            handler.handle(input);
        }
    }

}
