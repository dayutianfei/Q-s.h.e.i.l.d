package cn.dayutianfei.loadserver.server.source.http.handler;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.server.LoadServerCache;


public class CacheOperaServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(CacheOperaServlet.class);
    
    private static final long serialVersionUID = 1L;

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        /*
         * Get headers from http query string, and get body from http post
         * content.
         */
        int totalLength = request.getContentLength();
        if (totalLength <= 0) {
            LOG.warn("content length: " + totalLength);
        }
        response.setContentType("text/html");  
        response.setStatus(HttpServletResponse.SC_OK);
        
        String opera = request.getParameter("opera");
        if(null == opera){
            response.getWriter().println("<h1>Please choose your operation first!</h1>");
        }else if(LoadServerCache.OperaType.ADDHOST.toString().equalsIgnoreCase(opera.trim())){
            LoadServerCache.addBackendHost(request.getParameter("param"));
        }else if(LoadServerCache.OperaType.REMOVEHOST.toString().equalsIgnoreCase(opera.trim())){
            LoadServerCache.removeBackendHost(request.getParameter(request.getParameter("param")));
        }else if(LoadServerCache.OperaType.SHOWHOSTS.toString().equalsIgnoreCase(opera.trim())){
            if(LoadServerCache.hostName.isEmpty()){
                response.getWriter().println("<h1>no host in cache</h1>");
                response.getWriter().println("<h2>you can add a host name or ip use </h2>");
                response.getWriter().println("<h2>http://ip:port/cache?opera=operaName&param=paramValue</h2>");  
            }else{
                response.getWriter().println("<h1>hosts added are : </h1>");  
                for(String host: LoadServerCache.hostName){
                    response.getWriter().println("<h2>" + host + "</h2>");  
                }
            }
        }else{
            response.getWriter().println("<h1>Please choose your operation first!</h1>");
        }
        response.getWriter().println("session id : " + request.getSession(true).getId());  
        response.setCharacterEncoding(request.getCharacterEncoding());
        response.flushBuffer();
    }


    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        doPost(request, response);
    }
}
