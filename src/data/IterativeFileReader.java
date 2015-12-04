package data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import wiki.LinkPair;

/**
 * This is a utility class that reads batches of links in at a time.
 * It is designed to help in iterating through the links data file,
 * which has about 360 million links.
 * @author Eric
 *
 */
public class IterativeFileReader {
	private static int LINES_AT_ONCE = 1500000;
	BufferedReader reader = null;
	public IterativeFileReader(File f) throws FileNotFoundException {
		reader = new BufferedReader(new FileReader(f));
	}
	
	public List<LinkPair> readNextBatch() throws IOException {
		List<LinkPair> pairs = new ArrayList<LinkPair>();
		
		for (int i =0;i<LINES_AT_ONCE;i++) {
			String s = reader.readLine();
			if (s==null) {
				break;
			}
			LinkPair p = LinkPair.getPairFromDelimitedString(s);
			if (p!=null) {
				pairs.add(p);
			}
		}
		
		return pairs;
	}
	
	public void close() throws IOException {
		reader.close();
	}
}
