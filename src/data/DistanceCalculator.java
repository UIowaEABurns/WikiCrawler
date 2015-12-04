package data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

import wiki.LinkPair;

/**
 * This module does BFS starting from any desired article and calculates
 * how many new articles can be reached at steps 1, 2, 3... and so on.
 * @author Eric
 *
 */
public class DistanceCalculator {
	private static int START_WRITE_POINT = 3;
	public static void main(String[] args) throws Exception {
		HashMap<String, Integer> articlesToDistance = new HashMap<String, Integer>();
		File workingDir = new File(args[2]);
		workingDir.mkdirs();
		String STARTING_ARTICLE = args[1];
		File dataFile = new File(args[0]);
		articlesToDistance.put(STARTING_ARTICLE, 0);
		int nextDistance = 0;
		while (true) {
			File nextDataFile = new File(workingDir,nextDistance+".txt");
			BufferedWriter writer = new BufferedWriter(new FileWriter(nextDataFile));
			nextDistance+=1;
			System.out.println("distance = "+nextDistance);
			boolean found = false;
			IterativeFileReader reader = new IterativeFileReader(dataFile);
			while (true) {
				List<LinkPair> pairs = reader.readNextBatch();
				if (pairs.size()==0) {
					break;
				}
				
				for (LinkPair p : pairs) {
					if (articlesToDistance.containsKey(p.source) && !articlesToDistance.containsKey(p.dest)) {
						if (articlesToDistance.get(p.source)==(nextDistance-1)) {
							articlesToDistance.put(p.dest, nextDistance);
							found = true;
						}
					}
					if (!articlesToDistance.containsKey(p.dest) && nextDistance>START_WRITE_POINT) {
						writer.write(p.toString());
					}
				}
			}
			reader.close();
			writer.close();
			if (!found) {
				break;
			}
			if (nextDistance>START_WRITE_POINT) {
				dataFile = nextDataFile;
			}
		}
		
		int finalDistance = nextDistance-1;
		System.out.println("done with crawl, printing results");
		System.out.println("Maximum distance found was "+finalDistance);
		for (int i=0;i<=nextDistance;i++) {
			int counter = 0;
			for (String s : articlesToDistance.keySet()) {
				if (articlesToDistance.get(s)==i) {
					counter+=1;
				}
			}
			System.out.println("Articles at distance "+i+" = "+counter);
		}
		System.out.println("sample of max distance articles");
		int numArts = 0;
		for (String s : articlesToDistance.keySet()) {
			if (articlesToDistance.get(s)==finalDistance) {
				numArts+=1;
				if (numArts<10) {
					System.out.println(s);
				}
			}
		}
		System.out.println("Total number of articles reachable = "+articlesToDistance.size());
	}
}
