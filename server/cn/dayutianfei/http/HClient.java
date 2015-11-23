package cn.dayutianfei.http;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpExchange;

public class HClient {
	private static final Log logger = LogFactory.getLog(HClient.class);

	public static long begin_time_s;
	public static long total_B = 0;
	public static long total_records = 0;
	public static long total_msgs = 0;
	
	public static void main(String[] args) {
		logger.info("client is starting...");
		// final String server = args[0];
		final String server = "http://127.0.0.1:8081";
		
		HttpClient client = new HttpClient();
		client.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
		try {
			client.start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		// 获取schema
		ContentExchange exchange = new ContentExchange(false);
		exchange.setMethod("POST");
//		String url = server + "/infopub/?op=getDOS&schemaName=flow_schema";
		String url = server +"/hello"; //
		exchange.setURL(url);
		logger.info("URL : " + url);
		try {
		     client.send(exchange);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		int exchangeState = 0;
		try {
			exchangeState = exchange.waitForDone();
			if (exchangeState == HttpExchange.STATUS_COMPLETED) {
				String re = exchange.getResponseContent();
				System.out.println(re);
			} else if (exchangeState == HttpExchange.STATUS_EXCEPTED) {
				System.err.println("00_STATUS_EXCEPTED");
			} else if (exchangeState == HttpExchange.STATUS_EXPIRED) {
				System.err.println("01_STATUS_EXPIRED");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
	}
}
