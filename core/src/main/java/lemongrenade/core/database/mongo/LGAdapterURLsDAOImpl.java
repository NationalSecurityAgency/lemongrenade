package lemongrenade.core.database.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.WriteResult;
import lemongrenade.core.models.LGAdapterURLs;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.query.Query;

import java.util.Iterator;
import java.util.List;


public class LGAdapterURLsDAOImpl extends BasicDAO<LGAdapterURLs, ObjectId>
implements LGAdapterURLsDAO {


    public LGAdapterURLsDAOImpl(Class<LGAdapterURLs> entityClass, Datastore ds) {
        super(entityClass, ds);
    }

    public String getBaseUrlByAdapter(String adapter) {
        Query<LGAdapterURLs> query = createQuery().field("_id").equal(adapter);

        LGAdapterURLs adapberURL = query.get();

        return adapberURL.getBaseUrl();
    }

    public void saveNewBaseURL (String adapter, String baseURL) {
        LGAdapterURLs lgAdapterURLs = new LGAdapterURLs(adapter,baseURL);

        //todo: check against a regular expression
        //todo: try checking form and then raising exception if from is not right

//        Pattern pattern = Pattern.compile("@ (https?|ftp)://(-\\.)?([^\\s/?\\.#-]+\\.?)+(/[^\\s]*)?$");


//        Pattern pattern = Pattern.compile("(http://|https://)?([a-zA-Z0-9]+).*");
//        Matcher matcher = pattern.matcher(baseURL);
//
//        if (matcher.matches()) {
//
//            System.out.println("This pattern matches");
//        }
//        else {
//            System.out.println("This pattern does not match");
//        }


        savelgAdapterURLs(lgAdapterURLs);
    }

    @Override
    public void saveNewMultipleBaseURLs(String body) {

        JSONObject baseURLs = new JSONObject(body);


        System.out.println("body == " + body);
        Iterator<String> keys = baseURLs.keys();

        while (keys.hasNext()) {
            String adapter = keys.next();
            String baseUrl = baseURLs.getString(adapter);

            saveNewBaseURL(adapter,baseUrl);
        }
    }

    public LGAdapterURLs getLGAdapterURLByAdapter(String adapter) {
        Query<LGAdapterURLs> query = createQuery().field("_id").equal(adapter);

        LGAdapterURLs adapberURL = query.get();

        return adapberURL;
    }


    public void removeAllDocuments() {
        DBCollection LGAdapterURLs = createQuery().getCollection();
        DBCursor cursor = LGAdapterURLs.find();

        while (cursor.hasNext()) {

            LGAdapterURLs.remove(cursor.next());
        }
    }

    public List<LGAdapterURLs> getAll() {

        return createQuery().asList();
    }

    public JSONObject getAllJSON() {

        List <LGAdapterURLs> allLGAdapterURLs = getAll();
        JSONObject allLGAdapterURLsJSON = new JSONObject();

        for (LGAdapterURLs lgAdapterURLs : allLGAdapterURLs) {
            allLGAdapterURLsJSON.put(lgAdapterURLs.getAdapterName(),lgAdapterURLs.getBaseUrl());
        }

        return allLGAdapterURLsJSON;
    }

    public String getAllJSONString() {  return getAllJSON().toString();  }


    /**
     * @param LGAdapterURLs LGAdapterURLs
     */
    public void savelgAdapterURLs(LGAdapterURLs LGAdapterURLs) {
        getDatastore().save(LGAdapterURLs);
    }


    @Override
    public WriteResult delete(String adapter) {
        LGAdapterURLs urlByAdapter = getLGAdapterURLByAdapter(adapter);
        return super.delete(urlByAdapter);
    }


    /**
     * main
     * @param args Standard main args. Unused.
     */
    public static void main(String[] args) {


        LGAdapterURLsDAOImpl dao;
        MorphiaService ms;
        ms = new MorphiaService();
        dao = new LGAdapterURLsDAOImpl(LGAdapterURLs.class, ms.getDatastore());

//        dao.removeAllDocuments();

        dao.saveNewBaseURL("myAdapter", "https://myService.com");

        String newURL = dao.getBaseUrlByAdapter("myAdapter");

        System.out.println("The new URL is "+newURL);


        dao.saveNewBaseURL("myAdapter1", "https://meanie.com");
        dao.saveNewBaseURL("myAdapter2", "https://minie.com");
        dao.saveNewBaseURL("myAdapter3", "https://eeeenie.com");
        dao.saveNewBaseURL("myAdapter4", "https://moe.com");


        System.out.println(dao.getAllJSONString());







    }

}