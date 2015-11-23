package cn.dayutianfei.http;

import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class HClientT {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        DefaultHttpClient client = new DefaultHttpClient();
        String url = "http://localhost:8081";
        HttpPost post = new HttpPost();
        URI uri = URI.create(url+"/"+"infopub");
        post.setURI(uri);
        byte[] data  = new byte[1024*64];
        HttpEntity entity = new ByteArrayEntity(data);
        post.setEntity(entity);
        client.getParams().setParameter("http.socket.timeout", 60*1000);
        try {
            HttpResponse resp = client.execute(post);
            EntityUtils.consume(resp.getEntity());
            int codeReturned = resp.getStatusLine().getStatusCode();
            System.out.println(codeReturned);
        } catch (Exception e) {
            throw new IOException("send data to server error.", e);
        }
        return;
    }

}
