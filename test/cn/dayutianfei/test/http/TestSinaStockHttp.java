package cn.dayutianfei.test.http;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class TestSinaStockHttp {

	@SuppressWarnings("unused")
	private static HttpClient httpclient = new HttpClient();

	private static String DEFAULT_ENCODE = "UTF-8";// 默认字符编码

	public static String getContentFromUrl(String url)
			throws HttpException {
		String contentStr = "";
		// Create an instance of HttpClient.
		HttpClient client = new HttpClient();
		// Create a method instance.
		GetMethod method = new GetMethod(url);
		// Provide custom retry handler is necessary
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
				new DefaultHttpMethodRetryHandler(3, false));
		try {
			// Execute the method.
			int statusCode = client.executeMethod(method);
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: " + method.getStatusLine());
			}
			// Read the response body.
			byte[] responseBody = method.getResponseBody();
			// Deal with the response.
			// Use caution: ensure correct character encoding and is not binary
			// data
			contentStr = new String(responseBody,"gbk");

		} catch (HttpException e) {
			System.err.println("Fatal protocol violation: " + e.getMessage());
			throw new HttpException();
		} catch (IOException e) {
			System.err.println("Fatal transport error: " + e.getMessage());
			throw new HttpException();
		} finally {
			// Release the connection.
			method.releaseConnection();
		}
		//System.out.println(contentStr);
		return contentStr;
	}

	@SuppressWarnings("deprecation")
	public static String postContentToUrl(String content, String url)
			throws HttpException {
		String contentStr = "";
		HttpClient client = new HttpClient();
		PostMethod postMethod = new PostMethod(url);
		client.setConnectionTimeout(30000);
		try {
			postMethod.setRequestBody(content);
			int statusCode = client.executeMethod(postMethod);
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: "
						+ postMethod.getStatusLine());
			}
			byte[] responseBody = postMethod.getResponseBody();
			contentStr = new String(responseBody);
		} catch (HttpException e) {
			System.err.println("Fatal protocol violation: " + e.getMessage());
			throw new HttpException();
		} catch (IOException e) {
			System.err.println("Fatal transport error: " + e.getMessage());
			throw new HttpException();
		} finally {
			// Release the connection.
			postMethod.releaseConnection();
		}
		System.out.println(contentStr);
		return contentStr;
	}

	@SuppressWarnings("deprecation")
	public static String postXmlFileToUrl(String filePath, String url)
			throws HttpException {
		String contentStr = "";
		HttpClient client = new HttpClient();
		PostMethod postMethod = new PostMethod(url);
		client.setConnectionTimeout(30000);
		// Send any XML file as the body of the POST request
		try {
			File f = new File(filePath);
			// System.out.println("File Length = " + f.length());
			RequestEntity entity = new InputStreamRequestEntity(
					new FileInputStream(f), "text/xml; charset=utf-8");
			postMethod.setRequestEntity(entity);
			int statusCode = client.executeMethod(postMethod);
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: "
						+ postMethod.getStatusLine());
			}
			byte[] responseBody = postMethod.getResponseBody();
			contentStr = new String(responseBody);
		} catch (HttpException e) {
			System.err.println("Fatal protocol violation: " + e.getMessage());

		} catch (IOException e) {
			System.err.println("Fatal transport error: " + e.getMessage());
			throw new HttpException();
		} finally {
			// Release the connection.
			postMethod.releaseConnection();
		}
		return contentStr;

	}

	@SuppressWarnings("deprecation")
	public static String postFormContentToUrl(String[][] nameValuePair,
			String url, String encoding) throws HttpException {
		String contentStr = "";
		HttpClient client = new HttpClient();
		PostMethod postMethod = new PostMethod(url);
		client.setConnectionTimeout(30000);
		// Send any XML file as the body of the POST request
		try {
			if (nameValuePair != null) {
				NameValuePair[] nvps = new NameValuePair[nameValuePair.length];
				for (int i = 0; i < nameValuePair.length; i++) {
					nvps[i] = new NameValuePair(nameValuePair[i][0],
							nameValuePair[i][1]);
				}
				postMethod.setRequestBody(nvps);
			}
			int statusCode = client.executeMethod(postMethod);
			if (statusCode != HttpStatus.SC_OK) {
				System.err.println("Method failed: "
						+ postMethod.getStatusLine());
			}
			byte[] responseBody = postMethod.getResponseBody();
			contentStr = new String(responseBody, encoding);
		} catch (HttpException e) {
			System.err.println("Fatal protocol violation: " + e.getMessage());
			throw new HttpException();
		} catch (IOException e) {
			System.err.println("Fatal transport error: " + e.getMessage());
			throw new HttpException();
		} finally {
			// Release the connection.
			postMethod.releaseConnection();
		}
		return contentStr;
	}

	public static String postFormContentToUrl(String[][] nameValuePair,
			String url) throws HttpException {
		return postFormContentToUrl(nameValuePair, url, DEFAULT_ENCODE);
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {

		// "http://hq.sinajs.cn/list=股票代码"
		// 如：sh600000，sz000913 (这里sh是上海股市，sz为深圳股市)
		/*
		 * 这个字符串由许多数据拼接在一起，不同含义的数据用逗号隔开了，按照程序员的思路，顺序号从0开始。
0：”大秦铁路”，股票名字；
1：”27.55″，今日开盘价；
2：”27.25″，昨日收盘价；
3：”26.91″，当前价格；
4：”27.55″，今日最高价；
5：”26.20″，今日最低价；
6：”26.91″，竞买价，即“买一”报价；
7：”26.92″，竞卖价，即“卖一”报价；
8：”22114263″，成交的股票数，由于股票交易以一百股为基本单位，通常把该值除以一百；
9：”589824680″，成交金额，单位为“元”，通常以“万元”为成交金额的单位，所以通常把该值除以一万；
10：”4695″，“买一”申请4695股，即47手；
11：”26.91″，“买一”报价；
12：”57590″，“买二”
13：”26.90″，“买二”
14：”14700″，“买三”
15：”26.89″，“买三”
16：”14300″，“买四”
17：”26.88″，“买四”
18：”15100″，“买五”
19：”26.87″，“买五”
20：”3100″，“卖一”申报3100股，即31手；
21：”26.92″，“卖一”报价
(22, 23), (24, 25), (26,27), (28, 29)分别为“卖二”至“卖五的情况”
30：”2008-01-11″，日期；
31：”15:05:32″，时间；
		 */
		String[] re = getContentFromUrl("http://hq.sinajs.cn/list=sh600050").split(",");
		String[] re1 = getContentFromUrl("http://hq.sinajs.cn/list=sz002083").split(",");
		System.out.println(re[3]);
		System.out.println(re1[3]);
	}
}
