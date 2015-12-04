package wiki;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


public class Crawler {

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, IOException {
		Database.initialize();
		List<Thread> threads = new ArrayList<Thread>();
		ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<String>();
		for (String s : Database.getTopicsToTraverse()) {
			q.add(s);
		}
		System.out.println("founds this many topics to crawl at the start "+q.size());
		boolean verbose = false;
		for (int i=0;i<8;i++) {
			Connection con = new Connection(q, verbose);
			threads.add(con);
			con.start();
			verbose = false;
		}
		
		WriteThread writeThread = new WriteThread(q);
		writeThread.start();
		for (Thread  t : threads) {
			t.join();
		}
		writeThread.terminateOnceComplete = true;
		writeThread.join();
		System.out.println("done crawling. Terminating program");
	}
}