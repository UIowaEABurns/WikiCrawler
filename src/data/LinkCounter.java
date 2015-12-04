package data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.*;

import wiki.LinkPair;

/**
 * This class gets distributions of inbound and outbound links and also
 * finds the articles with the most inbound and outbound links.
 * @author Eric
 *
 */
public class LinkCounter {
	
	private static LinkData constructData(String s) {
		LinkData d = new LinkData();
		d.article = s;
		return d;
	}
	public static void main(String[] args) throws IOException {
		File dataOutputFile = new File(args[1]);
		dataOutputFile.delete();
		dataOutputFile.createNewFile();
		BufferedWriter writer = new BufferedWriter(new FileWriter(dataOutputFile));
		Map<String, LinkData> data = new HashMap<String, LinkData>();
		File dataFile = new File(args[0]);
		IterativeFileReader reader = new IterativeFileReader(dataFile);
		int linkCounter = 0;
		while (true) {
			List<LinkPair> pairs = reader.readNextBatch();
			if (pairs.size()==0) {
				break;
			}
			for (LinkPair p : pairs) {
				linkCounter+=1;
				if (!data.containsKey(p.source)) {
					data.put(p.source, constructData(p.source));
				}
				if (!data.containsKey(p.dest)) {
					data.put(p.dest, constructData(p.dest));
				}
				data.get(p.source).outbound+=1;
				data.get(p.dest).inbound+=1;
			}
			System.out.println(linkCounter);
		}
		reader.close();
		List<LinkData> dataList = new ArrayList<LinkData>();
		dataList.addAll(data.values());
		
		writer.write("Total number of articles = "+data.size()+"\n");
		writer.write("Total number of edges = "+linkCounter+"\n");
		Collections.sort(dataList, new LinkDataComparator(true));
		writer.write("top 10000 inbound\n");
		for (int i=0;i<10000;i++) {
			writer.write(dataList.get(i).article+" "+dataList.get(i).inbound+"\n");
		}
		Collections.sort(dataList, new LinkDataComparator(false));

		writer.write("\n\ntop 100 outbound\n");
		for (int i=0;i<100;i++) {
			writer.write(dataList.get(i).article+" "+dataList.get(i).outbound+"\n");
		}
		
		// maps # of links to count of articles
		HashMap<Integer, Integer> inboundMap = new HashMap<Integer,Integer>();
		HashMap<Integer, Integer> outboundMap = new HashMap<Integer,Integer>();
		for (LinkData d : dataList) {
			if (!inboundMap.containsKey(d.inbound)) {
				inboundMap.put(d.inbound, 0);
			}
			if (!outboundMap.containsKey(d.outbound)) {
				outboundMap.put(d.outbound, 0);
			}
			inboundMap.put(d.inbound, inboundMap.get(d.inbound)+1);
			outboundMap.put(d.outbound, outboundMap.get(d.outbound)+1);
		}
		writer.write("\n\ninbound map\n");
		for (Integer i : inboundMap.keySet()) {
			writer.write(i+ " "+inboundMap.get(i)+"\n");
		}
		
		writer.write("\n\noutbound map\n");
		for (Integer i : outboundMap.keySet()) {
			writer.write(i+ " "+outboundMap.get(i)+"\n");
		}
		
		writer.close();
	}
}
