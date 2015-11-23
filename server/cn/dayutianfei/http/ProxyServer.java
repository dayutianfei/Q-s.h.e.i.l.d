package cn.dayutianfei.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class ProxyServer {
	private static final Log logger = LogFactory.getLog(ProxyServer.class);
	private static Server server = null;

	public static void main(String[] args) {
		logger.info("server is starting...");
		InputStream input = ProxyServer.class
				.getResourceAsStream("httpserver.properties");
		Properties pros = new Properties();
		try {
			pros.load(input);
		} catch (IOException e1) {
			System.err.printf("%s ocurr in reading The configuration file:%s");
			System.exit(1);
		}

		// HTPP SERVER配置
		server = new Server();

		SelectChannelConnector connector = new SelectChannelConnector();
		int port = Integer.parseInt(pros.getProperty("http.port", "8080"));
		String host = pros.getProperty("http.host", "localhost");
		connector.setHost(host);
		connector.setPort(port);
		server.setConnectors(new Connector[] { connector });
		
		ContextHandler context_1 = new ContextHandler();
        context_1.setContextPath("/infopub");
        Handler handler_1 = new ServiceHandler();
        context_1.setHandler(handler_1);
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[]{context_1});
        
        ResourceHandler context_2 = new ResourceHandler(); 
        context_2.setDirectoriesListed(true);  
        context_2.setResourceBase("D:/temp");  
        context_2.setStylesheet("");
        
        ServletContextHandler context_3 = new ServletContextHandler(ServletContextHandler.SESSIONS);  
        context_3.setContextPath("/"); 
        // http://localhost:8080/hello  
        context_3.addServlet(new ServletHolder(new HelloServlet()), "/hello");  
        // http://localhost:8080/hello/kongxx  
        context_3.addServlet(new ServletHolder(new HelloServlet("Hello Kongxx!")), "/hello/kongxx");  
  
        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{contexts,context_2,context_3,new DefaultHandler()});
        server.setHandler(handlers);         
        
		start();
	}
	public static void start() {
        try {
            server.start();
            server.join();
        }
        catch (Exception e) {
            logger.error("Load System  Http Proxy Server start:" + e.getMessage());
        }
    }
}
