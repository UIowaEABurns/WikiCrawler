package wiki;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * This class is responsible for writing things out to the database.
 * @author Eric
 *
 */
public class WriteThread extends Thread {
	// This queue is used to save topics that still need to be visited. It is
	// expected to be a concurrent queue that is read by other threads.
	Queue topicsToCheck = null;
	
	// tells this thread to stop execution once there is nothing left to write out.
	public boolean terminateOnceComplete = false;
	public WriteThread(Queue topicsToCheck) {
		this.topicsToCheck = topicsToCheck;
	}
	
	// Thread running, just write out database results.
	@Override
	public void run() {
		try {
			this.writeLinkResultsForever(topicsToCheck);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * This function runs forever, periodically writing things out to the database.
	 * We wait about 2 seconds between writes to batch a decent number of rows,
	 * which makes writes much more efficient.
	 * @param topicsToCheck 
	 * @throws Exception
	 */
	public void writeLinkResultsForever(Queue<String> topicsToCheck) throws Exception {
		while ((!terminateOnceComplete) || (Database.finishedURLs.size()> 0 || Database.toWrite.size() > 0 || Database.canonWrites.size() > 0)) {
			int finishedReadSize = Database.finishedURLs.size();
			int readSize = Database.toWrite.size();
			System.out.println("writing out this many records to the links table "+readSize);
			boolean success = true;
			while (readSize>0) {
				List<LinkPair> pairs = new ArrayList<LinkPair>();
				for (int i=0;i<30000;i++) {
					pairs.add(Database.toWrite.poll());
					readSize--;
					if (readSize==0) {
						break;
					}
				}
				if (!Database.writeLinkResults(pairs)) {
					success=false;
				}
			}
			List<LinkPair> canon = new ArrayList<LinkPair>();
			int num = Database.canonWrites.size();
			for (int i=0;i<num;i++) {
				canon.add(Database.canonWrites.poll());
			}
			if (!Database.writeCanonNames(canon)) {
				success = false;
			}
			if (success) {
				List<String> urls = new ArrayList<String>();
				for (int i=0;i<finishedReadSize;i++) {
					urls.add(Database.finishedURLs.poll());
				}
				Database.writeFinishedURLs(urls);
			}
			if (topicsToCheck.size()<20000) {
				Set<String> topics = Database.getTopicsToTraverse();
				
				topicsToCheck.clear();
				topicsToCheck.addAll(topics);
			}
			Thread.sleep(2000);
		}
	}
}