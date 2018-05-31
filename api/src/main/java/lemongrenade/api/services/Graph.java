/*
This class serves as a proxy to query LEMONGRAPH via LEMONGRENADE.
All queries sent to <LEMONGRENADE>/lemongraph/ are passed to LEMONGRENADE and ther response returned.
 */

package lemongrenade.api.services;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;


@Path(Graph.path)
public class Graph {
    final static String path = "/lemongraph/";
    @Context
    HttpServletRequest request;
    @Context
    HttpServletResponse response;
    @Context
    ServletContext context;

    @GET
    @Path("{path:.*}")
    @Produces("application/json")
    public Response getForward(String body) {
        String extraPath = request.getPathInfo().substring(Graph.path.length());
        return Utils.lemongraphProxy(request, extraPath, body);
    }

    @POST
    @Path("{path:.*}")
    @Produces("application/json")
    public Response postForward(String body) {
        String extraPath = request.getPathInfo().substring(Graph.path.length());
        return Utils.lemongraphProxy(request, extraPath, body);
    }

    @PUT
    @Path("{path:.*}")
    @Produces("application/json")
    public Response putForward(String body) {
        String extraPath = request.getPathInfo().substring(Graph.path.length());
        return Utils.lemongraphProxy(request, extraPath, body);
    }

}
