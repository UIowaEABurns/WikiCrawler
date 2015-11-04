package wiki;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;


public class Main {

	public static void main(String[] args) throws InterruptedException, ClassNotFoundException, IOException {
		Database.initialize();
		List<Thread> threads = new ArrayList<Thread>();
		ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<String>();
		for (String s : Database.getTopicsToTraverse()) {
			q.add(s);
		}
		System.out.println("founds this many topics to check "+q.size());
		boolean verbose = false;
		for (int i=0;i<12;i++) {
			Connection con = new Connection(q, verbose);
			threads.add(con);
			con.start();
			verbose = false;
		}
		
		Thread writeThread = new WriteThread(q);
		writeThread.start();
		for (Thread  t : threads) {
			t.join();
		}
		System.out.println("finished crawling. Program is just sitting still not writing stuff now");
		writeThread.join();
		
	}
}