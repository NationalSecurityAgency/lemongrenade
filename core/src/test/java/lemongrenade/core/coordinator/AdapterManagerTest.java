package lemongrenade.core.coordinator;

import junit.framework.TestCase;

import lemongrenade.core.models.LGAdapterModel;
import lemongrenade.core.models.LGJob;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

public class AdapterManagerTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass());

    public void testAdapterDepthSelection() {
        AdapterManager am = new AdapterManager();
        am.deleteAll();
        LGAdapterModel a1 = new LGAdapterModel("001","testadapter1","n(value~/something/)",0,0, new JSONObject());
        am.addAdapter(a1);
        LGAdapterModel a2 = new LGAdapterModel("002","testadapter2","query2",2,0, new JSONObject());
        am.addAdapter(a2);
        LGAdapterModel a3 = new LGAdapterModel("003","testadapter3","query3",3,0, new JSONObject());
        am.addAdapter(a3);

        LGJob job = new LGJob();
        job.setJobId("j001");

        String rQuery = am.getGraphQueryForAdapter( "001", job,true);
        assertEquals(rQuery, "n(value~/something/)");

        // adapter depth should be used
        LGAdapterModel a5 = new LGAdapterModel("005","testadapter5","n(value~/something/)",5,0, new JSONObject());
        am.addAdapter(a5);
        String rQuery2 = am.getGraphQueryForAdapter("005", job,true);
        assertEquals(rQuery2, "n(value~/something/),1(depth<=5)");

        // set job config global value, that should be picked first
        JSONObject c = new JSONObject();
        c.put("depth",4);
        job.setJobConfig(c.toString());
        String rQuery3 = am.getGraphQueryForAdapter( "001", job,true);
        assertEquals(rQuery3, "n(value~/something/),1(depth<=4)");

        // set job depth for adapter in job_config value, that should be picked first
        JSONObject adapterconfig = new JSONObject();
        adapterconfig.put("depth",2);
        JSONObject adapters = new JSONObject();
        adapters.put("testadapter1",adapterconfig);
        c.put("adapters",adapters);
        job.setJobConfig(c.toString());

        String rQuery4 = am.getGraphQueryForAdapter( "001", job,true);
        assertEquals(rQuery4, "n(value~/something/),1(depth<=2)");
    }

    public void testAdapterFinds() {
        AdapterManager am = new AdapterManager();
        am.deleteAll();

        LGAdapterModel a1 = new LGAdapterModel("001","testadapter1","n(value~/something/)",0,0, new JSONObject());

        am.addAdapter(a1);
        LGAdapterModel a2 = new LGAdapterModel("002","testadapter2","n(value~/something/)",2,0, new JSONObject());
        HashMap<String, String> requiredKeys = new HashMap<String, String>();
        requiredKeys.put("hello", ".*");
        requiredKeys.put("hello2", ".*");
        a2.setRequiredKeys(requiredKeys);

        am.addAdapter(a2);
        LGAdapterModel a3 = new LGAdapterModel("003","testadapter3","n(value~/something/)",0,0, new JSONObject());
        am.addAdapter(a3);

        String graphQuery = new String("n(value~/something/)");
        LGJob job = new LGJob();
        String lookupGraphQuery = am.getGraphQueryForAdapter("003",job ,true);
        assertEquals(graphQuery,lookupGraphQuery);

        // Test getAdapterNameById
        String aName = am.getAdapterNameById("001");
        assertEquals(aName, "testadapter1");
        String aName3 = am.getAdapterNameById("003");
        assertEquals(aName3, "testadapter3");

        // call findBestAdapterByAdapterName()
        String id = am.findBestAdapterByAdapterName("testadapter1");
        assertEquals(id,"001");

        String id2= am.findBestAdapterByAdapterName("testadapter2");
        assertEquals(id2,"002");

        JSONObject diff = new JSONObject("{\"type\":\"id\",\"value\":\"e2ec2796-8d5f-404d-83dd-f839b8d3874c\",\"hello\":\"world\"}");
        List adapterList= am.buildUniqueAdapterListBasedOnRequiredKeys(diff);
        assertEquals(adapterList.get(0), "002");

        JSONArray uniquelist = am.getAdapterListNamesOnlyJson();
        int size = uniquelist.length();
        assertEquals(size,3);
        LGAdapterModel a4 = new LGAdapterModel("004","testadapter2","n(value~/something/)",2,0, new JSONObject());
        am.addAdapter(a4);

        // recheck size of unique list
        uniquelist = am.getAdapterListNamesOnlyJson();
        System.out.println(uniquelist.toString());
        size = uniquelist.length();
        assertEquals(size,3);
        am.deleteAll();
    }
}
