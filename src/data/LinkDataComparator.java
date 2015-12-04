package data;

import java.util.Comparator;

/**
 * Simple comparator for the LinkData class that allows us to sort
 * by either inbound or outbound links.
 * @author Eric
 *
 */
public class LinkDataComparator implements Comparator<LinkData> {

	boolean useInbound = true;
	public LinkDataComparator(boolean useInbound) {
		this.useInbound = useInbound;
	}
	
	@Override
	public int compare(LinkData arg0, LinkData arg1) {
		if (useInbound) {
			return arg1.inbound.compareTo(arg0.inbound);
		}
		return arg1.outbound.compareTo(arg0.outbound);
	}

}
