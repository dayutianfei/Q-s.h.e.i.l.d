package cn.dayutianfei.loadserver.client.deliver;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * http连接对象
 *
 * @author wzy
 * @date 2015-8-22 13:39:08
 */
public class HttpConnection extends Connection {
	
//	private final DefaultHttpClient client = new DefaultHttpClient();
    private static Logger LOG=LoggerFactory.getLogger(HttpConnection.class);
	private String url = null;

	public HttpConnection(String host, int port) {
		super(host+":"+port);
		this.url = "http://"+host+":"+port;
	}

    @Override
    public void open() throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void flush(HashMap<String,String> heads, byte[] data) throws IOException{
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost();
        URI uri = URI.create(url+"/"+"load");
        post.setURI(uri);
        HttpEntity entity = new ByteArrayEntity(data);
        post.setEntity(entity);
        client.getParams().setParameter("http.socket.timeout", 60*1000);
        for (String key : heads.keySet()) {
            post.setHeader(key, heads.get(key));
        }
        //post.setHeader("Connection", "keep-alive");
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
    
    @Override
    public boolean commit(HashMap<String,String> heads) throws IOException{
        DefaultHttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost();
        URI uri = URI.create(url+"/"+"load?oper=commit");
        post.setURI(uri);
        HttpEntity entity = new ByteArrayEntity("OK".getBytes());
        post.setEntity(entity);
        client.getParams().setParameter("http.socket.timeout", 60*1000);
        for (String key : heads.keySet()) {
            post.setHeader(key, heads.get(key));
        }
//        post.setHeader("Connection", "keep-alive");
        try {
            HttpResponse resp = client.execute(post);
            EntityUtils.consume(resp.getEntity());
            int codeReturned = resp.getStatusLine().getStatusCode();
            LOG.info("HTTP request get returned code : "+codeReturned);
        } catch (Exception e) {
            throw new IOException("send data to server error.", e);
        }
        return true;
    }
    
    @Override
    public boolean isActive() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

}
