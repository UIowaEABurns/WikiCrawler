package data;

import wiki.LinkPair;

/**
 * Simple utility class that stores how many inbound and outbound links
 * exist for a single article.
 * @author Eric
 *
 */
public class LinkData {
	public String article;
	public Integer inbound = 0;
	public Integer outbound = 0;
	
	@Override 
	public boolean equals(Object o) {
		if (!(o instanceof LinkData)) {
			return false;
		}
		LinkData p = (LinkData) o;
		
		return article.equals(p.article);
	}
	
	// Overriding this ensures that two identical links hash to the same value,
	// necessary for doing our HashSet-based duplication removal algorithm
	@Override
	public int hashCode() {
		return article.hashCode();
	}
}
