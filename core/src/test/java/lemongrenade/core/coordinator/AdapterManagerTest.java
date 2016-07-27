package lemongrenade.core.coordinator;

import junit.framework.TestCase;

import lemongrenade.core.coordinator.AdapterManager;
import lemongrenade.core.util.LGProperties;
import lemongrenade.core.models.LGJob;
import lemongrenade.core.util.LGConstants;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class AdapterManagerTest extends TestCase {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private transient Jedis jedis;


    @Test
    public void testAdapterDepthSelection() {
        AdapterManager am = new AdapterManager();
        jedis = new Jedis(LGProperties.get("redis.hostname"));

        jedis.flushAll();
        jedis.del(LGConstants.LGADAPTERSDATA,"001:requiredkeys");
        jedis.hset(LGConstants.LGADAPTERS,"001", "testadapter1");
        String t = new String(""+System.currentTimeMillis());
        jedis.hset(LGConstants.LGADAPTERSDATA,"001:hb",t);
        String graphQuery = new String("n(value~/something/)");
        jedis.hset(LGConstants.LGADAPTERSDATA,"001:graphquery",graphQuery);

        LGJob job = new LGJob();
        job.setJobId("j001");

        // No depth
        jedis.hset(LGConstants.LGADAPTERSDATA,"001:graphdepth","");
        String rQuery = am.getGraphQueryForAdapter("testadapter1", "001", job,true);
        assertEquals(rQuery, "n(value~/something/)");

        // adapter depth should be used
        jedis.hset(LGConstants.LGADAPTERSDATA,"001:graphdepth","5");
        String rQuery2 = am.getGraphQueryForAdapter("testadapter1", "001", job,true);
        assertEquals(rQuery2, "n(value~/something/),1(depth<=5)");

        // set job config global value, that should be picked first
        JSONObject c = new JSONObject();
        c.put("depth",4);
        job.setJobConfig(c.toString());
        String rQuery3 = am.getGraphQueryForAdapter("testadapter1", "001", job,true);
        assertEquals(rQuery3, "n(value~/something/),1(depth<=4)");

        // set job depth for adapter in job_config value, that should be picked first
        JSONObject adapterconfig = new JSONObject();
        adapterconfig.put("depth",2);
        JSONObject adapters = new JSONObject();
        adapters.put("testadapter1",adapterconfig);
        c.put("adapters",adapters);
        job.setJobConfig(c.toString());

        String rQuery4 = am.getGraphQueryForAdapter("testadapter1", "001", job,true);
        assertEquals(rQuery4, "n(value~/something/),1(depth<=2)");


        jedis.flushAll();

    }

    @Test
    public void testAdapterFinds() {
        AdapterManager am = new AdapterManager();
        jedis = new Jedis(LGProperties.get("redis.hostname"));

        jedis.flushAll();
        jedis.del(LGConstants.LGADAPTERSDATA,"001:requiredkeys");
        jedis.del(LGConstants.LGADAPTERSDATA,"002:requiredkeys");
        jedis.del(LGConstants.LGADAPTERSDATA,"003:requiredkeys");
        jedis.hset(LGConstants.LGADAPTERS,"001", "testadapter1");
        jedis.hset(LGConstants.LGADAPTERS,"002","testadapter2");
        jedis.hset(LGConstants.LGADAPTERS,"003","testadapter3");
        String t = new String(""+System.currentTimeMillis());
        jedis.hset(LGConstants.LGADAPTERSDATA,"001:hb",t);
        jedis.hset(LGConstants.LGADAPTERSDATA,"002:hb",t);
        jedis.hset(LGConstants.LGADAPTERSDATA,"003:hb","0001");
        ArrayList req= new ArrayList<String>(); req.add("hello");
        jedis.hset(LGConstants.LGADAPTERSDATA,"001:requiredkeys",new JSONArray(req).toString());
        String newId1 = "001"+":requiredkeys:"+"hello"+":keyvalue";
        jedis.hset(LGConstants.LGADAPTERSDATA, newId1, "world");


        ArrayList req2= new ArrayList<String>(); req2.add("status"); req2.add("someother");

        jedis.hset(LGConstants.LGADAPTERSDATA,"002:requiredkeys",new JSONArray(req2).toString());
        jedis.hset(LGConstants.LGADAPTERSDATA,"002:requiredkeys",new JSONArray(req2).toString());
        String newId2 = "002"+":requiredkeys:"+"status"+":keyvalue";
        jedis.hset(LGConstants.LGADAPTERSDATA, newId2, ".*");
        String newId3 = "002"+":requiredkeys:"+"someother"+":keyvalue";
        jedis.hset(LGConstants.LGADAPTERSDATA, newId3, ".*");

        jedis.hset(LGConstants.LGADAPTERSDATA,"003:requiredkeys",new JSONArray(req2).toString());
        String newId4 = "003"+":requiredkeys:"+"status"+":keyvalue";
        jedis.hset(LGConstants.LGADAPTERSDATA, newId4, ".*");
        String newId5 = "003"+":requiredkeys:"+"someother"+":keyvalue";
        jedis.hset(LGConstants.LGADAPTERSDATA, newId5, ".*");


        //jedis.hset("001:requiredkeys","hello",".*");
        //jedis.hset("002:requiredkeys","status",".*");
        //jedis.hset("003:requiredkeys","status",".*");

        //adapterId+":graphquery";
        String graphQuery = new String("n(value~/something/)");
        jedis.hset(LGConstants.LGADAPTERSDATA,"003:graphquery",graphQuery);
        LGJob job = new LGJob();
        String lookupGraphQuery = am.getGraphQueryForAdapter("testadapter3","003",job,true);
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
        assertEquals(adapterList.get(0), "001");

        JSONObject diff2 = new JSONObject("{\"type\":\"id\",\"value\":\"e2ec2796-8d5f-404d-83dd-f839b8d3874c\",\"status\":\"new\"}");
        List adapterList2= am.buildUniqueAdapterListBasedOnRequiredKeys(diff);

        assertTrue(!adapterList2.contains("002") && !adapterList2.contains("003") && (adapterList2.contains("001")));

        JSONArray uniquelist = am.getAdapterListNamesOnlyJson();
        int size = uniquelist.length();
        assertEquals(size,3);

        // Add another testadpater2
        jedis.hset(LGConstants.LGADAPTERS,"004","testadapter2");
        // recheck size of unique list
        uniquelist = am.getAdapterListNamesOnlyJson();
        size = uniquelist.length();
        assertEquals(size,3);


        jedis.flushAll();

    }
}
