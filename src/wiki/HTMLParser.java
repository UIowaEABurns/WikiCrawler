package wiki;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is responsible for parsing HTML received from Wikipedia and doing things
 * like finding links in it.
 * @author Eric
 *
 */
public class HTMLParser {

	/**
	 * Normalizes a wikipedia topic by doing things like removing anchors, arguments,
	 * and URL encoding.
	 * @param topic
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String normalizeTopic(String topic) throws UnsupportedEncodingException {
		
		topic = topic.substring(topic.lastIndexOf("/")+1);
		if (topic.contains("#")) {
			topic = topic.substring(0, topic.indexOf("#"));
		}
		if (topic.contains("?")) {
			topic = topic.substring(0, topic.indexOf("?"));
		}
		return URLDecoder.decode(topic, "UTF-8").replaceAll("\\?", "%3F");
	}
	
	/**
	 * Given a topic as stored in the database, returns the URL for the article
	 * for that topic.
	 * @param topic
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String getURLFromTopic(String topic) throws UnsupportedEncodingException {
		return "https://en.wikipedia.org/wiki/"+URLEncoder.encode(topic, "UTF-8");
	}
	
	// matches all images that we need to request from the server
	static Pattern urlPattern= Pattern.compile("href=\"/wiki/([^\"]*)\"");
	static Pattern titlePattern = Pattern.compile("<title>(.+) - Wikipedia, the free encyclopedia</title>");
	
	
	/**
	 * Returns the title of a Wikipedia page
	 * @param HTML HTML for a full wikipedia page
	 * @return
	 */
	public static String getCanonicalName(String HTML) {
		try {
			Matcher m = titlePattern.matcher(HTML);
			m.find();
			return m.group(1).replace(" ", "_");
		} catch (Exception e) {
			return null;
		}
		
	}
	
	private static List<String> getURLs(String HTML) {
		Matcher m = urlPattern.matcher(HTML);
		List<String> urls = new ArrayList<String>();
		while (m.find()) {
			urls.add(m.group(1));
		}
		return urls;
	}
	
	/**
	 * Given a topic, returns whether it is an actual Wikipedia topic. This will help filter out special
	 * pages and also the wikipedia main page and 404 page.
	 * @param topic
	 * @return
	 */
	public static boolean isTopic(String topic) {
		topic = topic.toLowerCase();
		String[] keywords = {"talk","file","template","template_talk", "help", "portal", "category", "wikipedia", "special", "topic",
				"draft", "file_talk", "user", "user_talk", "wikipedia_talk", "mediawiki", "mediawiki_talk", "book", "book_talk",
				"portal", "portal_talk", "module", "gadget", "module_talk", "draft_talk", "gadget_talk", "gadget_definition"};
		if (topic.equals("main page") || topic.equals("main_page") || topic.equals("Bad_title") || topic.equals("bad_title")) {
			return false;
		}
		for (String k : keywords) {
			if (topic.startsWith(k+":")) {
				return false;
			}
		}
		return true;
	}
	
	public static Set<String> getTopicsFromHTML(String HTML) throws UnsupportedEncodingException {
		List<String> initialUrls = getURLs(HTML);
		Set<String> finalUrls = new HashSet<String>();
		
		for (String s : initialUrls) {
			if (isTopic(s)) {
				finalUrls.add(normalizeTopic(s));
			}
		}
		
		return finalUrls;
	}
	
	static Pattern nextPagePattern= Pattern.compile("href=\"/w/([^\"]*)\" title=\"Category:All orphaned articles\">next page");

	/**
	 * Used to get the next page of orphans
	 * @param HTML
	 * @return
	 */
	public static String getNextPageURL(String HTML) {
		Matcher m = nextPagePattern.matcher(HTML);
		m.find();
		return m.group(1).replace("&amp;", "&");
	}
}
