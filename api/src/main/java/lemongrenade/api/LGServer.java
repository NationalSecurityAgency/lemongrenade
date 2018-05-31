package lemongrenade.api;

import lemongrenade.core.util.LGProperties;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

/**
 *
 */
public class LGServer {

    public static void main(String[] args) throws Exception {
        int port = LGProperties.getInteger("api.port",9999);
        int idleTimeout = LGProperties.getInteger("api.idle_timeout",120000);//2 minutes default
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        Server jettyServer = new Server();

        //add connector
        ServerConnector http = new ServerConnector(jettyServer);
        http.setPort(port);
        http.setIdleTimeout(idleTimeout);
        jettyServer.addConnector(http);

        //add handler
        jettyServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        // Services are all under lemongrenade.api.services
        jerseyServlet.setInitParameter("jersey.config.server.provider.packages", "lemongrenade.api.services");

        try {
            System.out.println("Starting REST API Server");
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jettyServer.stop();
            jettyServer.destroy();
        }
    }
}
