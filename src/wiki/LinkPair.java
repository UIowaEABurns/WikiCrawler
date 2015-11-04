package wiki;

/**
 * This is a very simple class that represents a link from source to destination
 * @author Eric
 *
 */
public class LinkPair {
	public String source;
	public String dest;
	
	@Override 
	public boolean equals(Object o) {
		if (!(o instanceof LinkPair)) {
			return false;
		}
		LinkPair p = (LinkPair) o;
		
		return p.source.equals(source) && p.dest.equals(dest);
	}
	
	// Overriding this ensures that two identical links hash to the same value,
	// necessary for doing our HashSet-based duplication removal algorithm
	@Override
	public int hashCode() {
		return (source.hashCode() + dest.hashCode()) % Integer.MAX_VALUE;
	}
	
	public int getTableHash(int maximum) {
		return Math.abs(dest.hashCode()) % maximum;
	}
	
	// The string ?:,;? is just a delimiter that does not appear in any link, which
	// we need when writing to disk to ensure we can read links back in correctly
	public String toString() {
		return source+"?:,;?"+dest+"\n";
	}
	
	public static LinkPair getPairFromDelimitedString(String s ){
		s = s.trim();
		LinkPair p = new LinkPair();
		
		String[] comps = s.split("\\?:,;\\?");
		if (comps.length<2) {
			return null;
		}
		p.source = comps[0].trim();
		p.dest = comps[1].trim();
		return p;
	}
}
