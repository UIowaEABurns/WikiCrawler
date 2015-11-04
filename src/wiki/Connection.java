package wiki;

import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;
import java.util.Set;


import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.AbstractHttpMessage;
import org.apache.http.util.EntityUtils;


public class Connection extends Thread {
	HttpClient client=null;
	private Queue<String> topicQueue = null;
	private boolean verbose;
	public Connection(Queue<String> topicQueue, boolean verbose) {
		this.topicQueue=topicQueue;
		this.verbose=verbose;
	}
	
	@Override
	public void run() {
		this.client=new DefaultHttpClient();
		try {
			this.crawl();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void log(String s) {
		if (verbose) {
			System.out.println(s);
		}
	}
	
	public String downloadPage(String topic) throws ClientProtocolException, IOException {
		String url = HTMLParser.getURLFromTopic(topic);
		return downloadPageWithURL(url);
	}
	
	public String downloadPageWithURL(String url) throws ParseException, IOException {
		HttpGet get=new HttpGet(url);
		get = (HttpGet) setHeaders(get);
		RequestConfig defaultRequestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.BEST_MATCH).setExpectContinueEnabled(true).setStaleConnectionCheckEnabled(true).setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST)).setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC)).build();
		RequestConfig requestConfig = RequestConfig.copy(defaultRequestConfig).setSocketTimeout(5000).setConnectTimeout(5000).setConnectionRequestTimeout(5000).build();
		get.setConfig(requestConfig);
		log("executing get");
		HttpResponse response=client.execute(get);
		String HTML = EntityUtils.toString(response.getEntity());
		response.getEntity().getContent().close();
		return HTML;
	}
	
	public void crawl() throws InterruptedException {
			String url = null;
			String topic = null;
			while (true) {
				topic = topicQueue.poll();
				
				// waiting for the database thread to write things out.
				if (topic==null) {
					System.out.println("No topics to retrieve");
					Thread.sleep(60000);
					continue;
				}
				log(topic);
				try {
					// try to ensure we don't get too far ahead of the database
					// thread by waiting for it to catch up
					while (Database.getPendingSize()>60000) {
						log("waiting for database");
						Thread.sleep(2000);
					}
					String HTML = downloadPage(topic);
					
					//some links are bad-- we want to detect this and stop looking for links.
					if (HTML.contains("Wikipedia does not have an article with this exact name")) {
						log("bad topic " +topic);
						Database.saveFinishedTopic(topic);
						continue;
					}

					Set<String> topics = HTMLParser.getTopicsFromHTML(HTML);
					log("finished parsing: found this many topics "+topics.size());
					String canon = HTMLParser.getCanonicalName(HTML);
					if (canon==null) {
						canon = topic;
					}
					Database.saveLinkResults(canon, topics, topic);	
					log("results saved");
				} catch (Exception e) {
					// exceptions are generally transient network errors-- just refresh the client,
					// wait a bit, and move on. The topic will eventually be revisited.
					topicQueue.add(topic);
					client= new DefaultHttpClient();
					e.printStackTrace();
					System.out.println(url);
					Thread.sleep(10000);
				}
			}		
	}
	
	private AbstractHttpMessage setHeaders(AbstractHttpMessage msg) {
		String cookieString="killmenothing;";
		
		msg.addHeader("Cookie",cookieString);
		msg.addHeader("Connection", "keep-alive");
		msg.addHeader("Accept-Language","en-US,en;q=0.5");
		msg.addHeader("User-Agent","Mozilla/5.0 (Windows NT 6.3; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0");
		
		return msg;
	}
	
	public Connection() throws IOException {		
		client=new DefaultHttpClient();	
	}
}