package data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

import wiki.Database;
import wiki.LinkPair;

/**
 * Generates random subsets of the Wikipedia graph, each with approximately 5000 articles.
 * Each subset contains all the links between the randomly chosen articles. Also produces
 * a subset using the Top5000 file.
 * @author Eric
 *
 */

public class RandomSubsetGenerator {
	static int NUM_SUBSETS = 5;
	static double INCLUSION_PROBABILITY = 0.001;
	static Random rand = new Random();
	public static void main(String[] args) throws Exception {
		Database.initialize();
		List<HashSet<String>> subsets = new ArrayList<HashSet<String>>();
		File outputDir = new File(args[1], "links");

		int NUM_SUBSETS = 5;
		
		for (int i=0;i<NUM_SUBSETS;i++) {
			subsets.add(new HashSet<String>());
		}
		
		for(String s : Database.readCanonTopics()) {
			for (int i=0;i<NUM_SUBSETS; i++) {
				if (rand.nextDouble()<0.002) {
					subsets.get(i).add(s);
				}
			}
		}
		
		int i=0;
		
		NUM_SUBSETS+=1;
		subsets.add(Top5000.getPopular());
		
		List<List<LinkPair>> linkSubsets = new ArrayList<List<LinkPair>>();
		for (i=0;i<NUM_SUBSETS;i++) {
			linkSubsets.add(new ArrayList<LinkPair>());
		}
		File dataFile = new File(args[0]);
		IterativeFileReader reader = new IterativeFileReader(dataFile);
		while (true) {
			List<LinkPair> pairs = reader.readNextBatch();
			if (pairs.size()==0) {
				break;
			}
			for (LinkPair p : pairs) {
				for (i=0;i<NUM_SUBSETS;i++) {
					HashSet<String> curSet = subsets.get(i);

					if (curSet.contains(p.source)&&curSet.contains(p.dest)) {
						linkSubsets.get(i).add(p);
					}
				}
			}
		}
		
		outputDir.mkdirs();
		for (i=0;i<NUM_SUBSETS;i++) {
			File outputFile = new File(outputDir, "links_"+(i+1)+".csv");
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
			for (LinkPair p : linkSubsets.get(i)) {
				writer.write(p.toString().replace(';', '_').replace('|','_').replace(",", "_").replace("?:__?", ";"));
			}
			writer.close();
		}
	}
}
