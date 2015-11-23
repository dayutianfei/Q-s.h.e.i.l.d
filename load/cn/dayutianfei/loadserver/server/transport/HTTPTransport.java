package cn.dayutianfei.loadserver.server.transport;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.dayutianfei.loadserver.client.cache.ClientCache;

public class HTTPTransport {
    
    private static final Logger LOG = LoggerFactory.getLogger(HTTPTransport.class);

    public static void send(String oper ,String hostName) throws IOException{
        DefaultHttpClient client = new DefaultHttpClient();
        String url = "http://"+hostName+ClientCache.HTTP_SOURCE_PORT;
        HttpPost post = new HttpPost();
        URI uri = URI.create(url+"/"+"cache?"+oper+"="+hostName);
        post.setURI(uri);
        HttpEntity entity = new ByteArrayEntity(hostName.getBytes());
        post.setEntity(entity);
        client.getParams().setParameter("http.socket.timeout", 60*1000);
        try {
            HttpResponse resp = client.execute(post);
            EntityUtils.consume(resp.getEntity());
            int codeReturned = resp.getStatusLine().getStatusCode();
            LOG.info("HTTP request get returned code : "+codeReturned);
        } catch (Exception e) {
            throw new IOException("send data to server error.", e);
        }
        return;
    }
}
