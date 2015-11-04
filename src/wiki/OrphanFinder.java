package wiki;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Gets the list of orphaned articles.
 * @author Eric
 *
 */
public class OrphanFinder {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Database.initialize();
		Connection con = new Connection();
		String HTML = con.downloadPageWithURL("https://en.wikipedia.org/wiki/Category:All_orphaned_articles");
		Set<String> topics = new HashSet<String>();
		while (true) {
			topics.addAll(HTMLParser.getTopicsFromHTML(HTML));
			if (topics.size()==0) {
				break;
			}
			if (Database.writeOrphans(topics)) {
				topics.clear();
			}
			
			String nextURL = "https://en.wikipedia.org/w/"+HTMLParser.getNextPageURL(HTML);
			HTML = con.downloadPageWithURL(nextURL);
			System.out.println(nextURL);
			
		}
	}
}
