package lemongrenade.core.api;

import lemongrenade.core.util.LGProperties;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;

public class LGServer {

    public static void main(String[] args) {

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        Server jettyServer = new Server(LGProperties.getInteger("api_server_port",9999));
        jettyServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        // Services are all under lemongrenade.core.api.services
        jerseyServlet.setInitParameter("jersey.config.server.provider.packages", "lemongrenade.core.api.services");

        try {
            System.out.println("Starting REST Api Server");
            jettyServer.start();
            jettyServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jettyServer.destroy();
        }
    }
}
