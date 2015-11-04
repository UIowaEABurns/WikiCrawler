package wiki;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.commons.io.FileUtils;

public class DuplicateRemover {
	
	private static int NUMBER_OF_TABLES = 300;
	private static int RECORDS_PER_ITERATION = 2000000;
	private static int STARTING_RECORD = 672000000;
	private static File parentDirectory = new File("C:/users/eric/desktop/indexed_links");
	
	private static File getFile(int num) throws IOException {
		File f = new File(parentDirectory, String.valueOf(num)+".txt");
		if (!f.exists()) {
			f.createNewFile();
		}
		return f;
	}
	
	private static String linksToString(Collection<LinkPair> pairs) {
		StringBuilder sb = new StringBuilder();
		for (LinkPair p : pairs) {
			sb.append(p.toString());
		}
		
		return sb.toString();
	}
	
	public static void appendToFile(File f, Collection<LinkPair> pairs) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
		bw.write(linksToString(pairs));
		bw.close();
	}
	
	public static void writeToHashedFiles(Collection<LinkPair> pairs) throws IOException {
		HashMap<Integer, List<LinkPair>> hashToLinks = new HashMap<Integer, List<LinkPair>>();
		for (int i =0 ;i<NUMBER_OF_TABLES;i++) {
			hashToLinks.put(i, new ArrayList<LinkPair>());
		}
		System.out.println("found this many pairs in total: "+pairs.size());
		for (LinkPair p : pairs) {
			List<LinkPair> curPairs = hashToLinks.get(p.getTableHash(NUMBER_OF_TABLES));
		    curPairs.add(p);
		}
		for (int i=0;i<NUMBER_OF_TABLES;i++) {
			appendToFile(getFile(i), hashToLinks.get(i));
			System.out.println("writing this many pairs to table "+i+": "+hashToLinks.get(i).size());
		}
	}
	
	public static void readFromDB() throws IOException {
		while (true) {
			System.out.println("current record is "+STARTING_RECORD);
			Set<LinkPair> pairs = Database.readLinks(STARTING_RECORD, RECORDS_PER_ITERATION);
			if (pairs.size()==0) {
				break;
			}
			writeToHashedFiles(pairs);
			
			STARTING_RECORD+=RECORDS_PER_ITERATION;
		}
	}
	
	public static void canonize(Map<String, String> canon, List<LinkPair> pairs) {
		for (LinkPair p: pairs) {
			if (canon.containsKey(p.source)) {
				p.source = canon.get(p.source);
			}
			if (canon.containsKey(p.dest)) {
				p.dest = canon.get(p.dest);
			}
		}
	}
	
	public static void readFromFile(File f) throws IOException {
		parentDirectory.mkdirs();
		Map<String, String> canon = Database.readCanon();
		BufferedReader reader = new BufferedReader(new FileReader(f));
		while (true) {
			List<LinkPair> pairs = new ArrayList<LinkPair>();
			boolean done = false;
			for (int i =0;i<RECORDS_PER_ITERATION;i++) {
				String s = reader.readLine();
				if (s==null) {
					done = true;
					break;
				}
				LinkPair p = LinkPair.getPairFromDelimitedString(s);
				if (p!=null) {
					pairs.add(p);
				}
			}
			System.out.println("got this many new links " +pairs.size());
			canonize(canon, pairs);
			writeToHashedFiles(pairs);			
			
			if (done) {
				break;
			}
		}
	}
	
	public static void compileIntoSingleFile() throws IOException {
		File output= new File("C:/users/eric/desktop/links/output_canon.txt");
		output.getParentFile().mkdirs();
		output.createNewFile();
		int totalCount = 0;
		for (File f : parentDirectory.listFiles()) {
			HashSet<LinkPair> pairs = new HashSet<LinkPair>();
			String[] links = FileUtils.readFileToString(f).split("\n");
			System.out.println("found this many links in "+f.getName()+": "+links.length);
			for (String s : links) {
				LinkPair p = LinkPair.getPairFromDelimitedString(s);
				if (p!=null && HTMLParser.isTopic(p.source) && HTMLParser.isTopic(p.dest)) {
					pairs.add(p);
				}
			}
			System.out.println("found this many unique links: "+pairs.size());
			totalCount+=pairs.size();
			appendToFile(output, pairs);
		}
		System.out.println("found this many links in total " + totalCount);
	}
	
	public static void main(String[] args) throws IOException {
		compileIntoSingleFile();
	}
}
