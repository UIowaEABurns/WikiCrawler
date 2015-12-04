package wiki;


import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;



/**
 * Notes that the initialize method must be called before doing anything else in this class.
 */
public class Database {		
	private static String username = null;
	private static String password = null;
	// Running list of things that need to be written out to the links table
	public static Queue<LinkPair> toWrite = new ConcurrentLinkedQueue<LinkPair>();
	// Running list of things that need to be written out to the finished table
	public static Queue<String> finishedURLs = new ConcurrentLinkedQueue<String>();
	// Running list of things that need to be written out to the canon table
	public static Queue<LinkPair> canonWrites = new ConcurrentLinkedQueue<LinkPair>();
	/**
	 * @return a new connection to the database from the connection pool
	 */
	protected synchronized static Connection getConnection() throws SQLException {
		try {
			Connection con= DriverManager.getConnection("jdbc:mysql://localhost/wikipedia?autoReconnect=true&amp;zeroDateTimeBehavior=convertToNull&amp",username, password);
			return con;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
		
	}
	
	/**
	 * Sets up the jdbc driver for use
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void initialize() throws ClassNotFoundException, IOException {	
		
		File config = new File(Database.class.getResource("/config/database.txt").getFile());
		String[] settings = FileUtils.readFileToString(config).split("\n");
		username = settings[0].split("=")[1].trim();
		password = settings[1].split("=")[1].trim();
		
		Class.forName("com.mysql.jdbc.Driver");
	}
	
	protected static void safeClose(CallableStatement statement) {
		try {
			if (statement!=null) {
				statement.close();
			}
		
		} catch (Exception e) {
		}	
	}
	
	/**
	 * Method which safely closes a connection pool connection
	 * and doesn't raise any errors
	 * @param c The connection to safely close
	 */
	protected static synchronized void safeClose(Connection c) {
		try {
			if(c != null && !c.isClosed()) {
				c.close();				
			}
		} catch (Exception e){
		}

	}

	/**
	 * Method which closes a result set
	 * @param r The result set close
	 */
	protected static void safeClose(ResultSet r) {
		try {
			if(r != null && !r.isClosed()) {
				r.close();
			}
		} catch (Exception e){
		}
	}
	
	/**
	 * Encodes the given string so that it is database safe. In our case,
	 * that just means escaping \ and "
	 * @param s
	 * @return
	 */
	private static String dbEncode(String s) {
		return s.replace("\\", "\\\\").replace("\"","\\\"");
	}
	
	private static String compileLinkString(List<LinkPair> pairs) {
		if (pairs.size()==0) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		for (LinkPair pair : pairs) {
			sb.append("(\"");
			sb.append(dbEncode(pair.source));
			sb.append("\",\"");
			sb.append(dbEncode(pair.dest));
			sb.append("\"),");
		}
		return sb.substring(0, sb.length()-1);
	}
	
	private static String compileFinishedString(Collection<String> strs) {
		StringBuilder sb = new StringBuilder();
		for (String p : strs) {
			sb.append("(\"");
			sb.append(dbEncode(p));
			sb.append("\"),");
		}
		
		return sb.substring(0, sb.length()-1);
	}
	
	private static String compileTopicString(List<LinkPair> pairs) {
		StringBuilder sb = new StringBuilder();
		for (LinkPair p : pairs) {
			sb.append("(\"");
			sb.append(dbEncode(p.dest));
			sb.append("\"),");
		}
		for (LinkPair p : pairs) {
			sb.append("(\"");
			sb.append(dbEncode(p.source));
			sb.append("\"),");
		}
		return sb.substring(0, sb.length()-1);
	}
	
	public static boolean writeTopics(Collection<String> topics) {
		Connection con = null;
		CallableStatement procedure = null;
		String topicString = compileFinishedString(topics);
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("INSERT IGNORE INTO topics VALUES "+topicString);
				procedure.executeUpdate();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
			}
		return false;
	}
	
	public static boolean writeLinkResults(List<LinkPair> pairs) throws IOException {
		Connection con = null;
		CallableStatement procedure = null;
		String topicString = compileTopicString(pairs);
		String linkString = compileLinkString(pairs);
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("INSERT IGNORE INTO topics VALUES "+topicString);
				procedure.executeUpdate();
				Database.safeClose(procedure);
				procedure = con.prepareCall("INSERT IGNORE INTO links VALUES "+linkString);
				procedure.executeUpdate();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
			}
		return false;
	}
	
	public static boolean writeFinishedURLs(List<String> strs) {
		if (strs.size()==0) {
			return true;
		}
		Connection con = null;
		CallableStatement procedure = null;
		String topicString = compileFinishedString(strs);
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("INSERT IGNORE INTO finished VALUES "+topicString);
				procedure.executeUpdate();
				
				return true;
			} catch (Exception e) {
				System.out.println(topicString);
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
			}
		return false;
	}
	
	public static boolean writeCanonNames(List<LinkPair> pairs) {
		if (pairs.size()==0) {
			return true;
		}
		Connection con = null;
		CallableStatement procedure = null;
		String linkString = compileLinkString(pairs);
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("INSERT IGNORE INTO canon VALUES "+linkString);
				procedure.executeUpdate();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
			}
		return false;
	}
	
	protected synchronized static void saveLinkResults(String source, Set<String> dests, String originalSource) {
		List<LinkPair> pairs = new ArrayList<LinkPair>();
		for (String s : dests) {
			LinkPair p = new LinkPair();
			p.source=source;
			p.dest=s;
			pairs.add(p);
		}
		if (dests.size()==0) {
			LinkPair p = new LinkPair();
			p.source=source;
			p.dest="FAKE_PLACEHOLDER_ARTICLE";
			pairs.add(p);
		}
		toWrite.addAll(pairs);
		finishedURLs.add(originalSource);
		LinkPair canon = new LinkPair();
		canon.source=originalSource;
		canon.dest = source;
		canonWrites.add(canon);
	}
	
	/**
	 * Gets the number of links that are in memory and have not yet been 
	 * written to the database.
	 */
	public static int getPendingSize() {
		return toWrite.size();
		
	}
	
	protected synchronized static void saveFinishedTopic(String topic) {
		finishedURLs.add(topic);
		LinkPair canon = new LinkPair();
		canon.source=topic;
		canon.dest = topic;
		canonWrites.add(canon);
		
	}
	
	protected static Set<String> getTopicsToTraverse() {
		Connection con = null;
		CallableStatement procedure = null;
		ResultSet results = null;
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("select * from topics where topic  not in (select * from finished) limit 50000");
				results = procedure.executeQuery();
				Set<String> strs = new HashSet<String>();
				while (results.next()) {
					strs.add(results.getString("topic"));
				}
				return strs;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
				Database.safeClose(results);
			}
		return null;
	}
	
	public static Collection<String> readCanonTopics() {
		Connection con = null;
		CallableStatement procedure = null;
		ResultSet results = null;
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("select canon_name from canon");
				results = procedure.executeQuery();
				Set<String> pairs = new HashSet<String>();
				while (results.next()) {
					pairs.add(results.getString("canon_name"));
				}
				return pairs;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
				Database.safeClose(results);
			}
		return null;
	}
	
	public static List<String> readTopics() {
		Connection con = null;
		CallableStatement procedure = null;
		ResultSet results = null;
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("select * from topics where topic not in (select link_name from canon)");
				results = procedure.executeQuery();
				List<String> pairs = new ArrayList<String>();
				while (results.next()) {
					pairs.add(results.getString("topic"));
				}
				return pairs;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
				Database.safeClose(results);
			}
		return null;
	}
	
	public static Set<LinkPair> readLinks(int startRecord, int limit) {
		Connection con = null;
		CallableStatement procedure = null;
		ResultSet results = null;
			try {
				con = Database.getConnection();
				procedure = con.prepareCall("select * from links limit "+startRecord+","+limit);
				results = procedure.executeQuery();
				Set<LinkPair> pairs = new HashSet<LinkPair>();
				while (results.next()) {
					LinkPair p = new LinkPair();
					p.source = results.getString("source");
					p.dest = results.getString("dest");
					pairs.add(p);
				}
				return pairs;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
				Database.safeClose(results);
			}
		return null;
	}
	
	public static Map<String,String> readCanon() {
		Connection con = null;
		CallableStatement procedure = null;
		ResultSet results = null;
			try {
				int startRecord=0;
				int recordsPerRead = 3500000;
				HashMap<String,String> linkToCanon = new HashMap<String,String>();
				while (true) {
					con = Database.getConnection();
					procedure = con.prepareCall("select link_name, canon_name from canon limit "+startRecord+","+recordsPerRead);
					results = procedure.executeQuery();
					boolean foundRecord = false;
					while (results.next()) {
						String link = results.getString("link_name").trim();
						String canon = results.getString("canon_name").trim();
						if (!link.equals(canon)) {
							linkToCanon.put(link, canon);
						}
						foundRecord=true;
					}
					Database.safeClose(results);
					Database.safeClose(procedure);
					if (!foundRecord) {
						break;
					}
					startRecord+=recordsPerRead;
				}
				
				return linkToCanon;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Database.safeClose(con);
				Database.safeClose(procedure);
				Database.safeClose(results);
			}
		return null;
	}
	
	
}
