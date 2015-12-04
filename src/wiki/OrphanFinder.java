package wiki;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Gets the list of orphaned articles and adds them to the "topics" table in the database
 * @author Eric
 *
 */
public class OrphanFinder {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Database.initialize();
		Connection con = new Connection();
		// This URL points to the first page for orphaned wikipedia articles
		String HTML = con.downloadPageWithURL("https://en.wikipedia.org/wiki/Category:All_orphaned_articles");
		Set<String> topics = new HashSet<String>();
		while (true) {
			topics.addAll(HTMLParser.getTopicsFromHTML(HTML));
			if (topics.size()==0) {
				break;
			}
			if (Database.writeTopics(topics)) {
				topics.clear();
			}
			
			String nextURL = "https://en.wikipedia.org/w/"+HTMLParser.getNextPageURL(HTML);
			System.out.println("working on URL "+nextURL);
			HTML = con.downloadPageWithURL(nextURL);			
		}
	}
}
