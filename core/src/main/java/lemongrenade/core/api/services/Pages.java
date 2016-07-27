package lemongrenade.core.api.services;

import lemongrenade.core.util.LGProperties;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;


@Path("/")
public class Pages {

    /*
    @GET
    @Path("{path:.*}")
    public InputStream Get(@PathParam("path") String path) {
        path.replaceAll("..","");  // don't trust source
        String apiRootPath = LGProperties.get("api.rootpath","/opt/LEMONGRENADE/static/");
        if (path.equals("")) {
            path = "index.html";
        }
        File f = new File(apiRootPath+File.separator+path);

        InputStream r = null;
        try {
            r = new FileInputStream(f);
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return r;
    }
    */

}
