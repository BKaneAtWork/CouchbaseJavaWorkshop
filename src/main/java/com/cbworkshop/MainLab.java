package com.cbworkshop;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.error.*;
import com.couchbase.client.java.error.subdoc.*;
import com.couchbase.client.core.tracing.ThresholdLogTracer;
import com.couchbase.client.core.tracing.ThresholdLogReporter;
import com.couchbase.client.encryption.AES256CryptoProvider;
import com.couchbase.client.encryption.CryptoManager;
import com.couchbase.client.encryption.JceksKeyStoreProvider;

import rx.Observable;

import static com.couchbase.client.java.query.Select.select;

import io.opentracing.Tracer;

public class MainLab {
	
	public static final String CMD_QUIT = "quit";
	public static final String CMD_CREATE = "create";
	public static final String CMD_READ = "read";
	public static final String CMD_UPDATE = "update";
	public static final String CMD_SUBDOC = "subdoc";
	public static final String CMD_DELETE = "delete";
	public static final String CMD_QUERY = "query";
	public static final String CMD_QUERY_ASYNC = "queryasync";
	public static final String CMD_QUERY_AIRPORTS = "queryairports";
	public static final String CMD_BULK_WRITE = "bulkwrite";
	public static final String CMD_BULK_WRITE_SYNC = "bulkwritesync";
	public static final String CMD_SEARCH = "search";
	public static final String CMD_ENCRYPT = "fleput";
	public static final String CMD_ENCRYPT_GET = "fleget";
	public static final String CMD_UPSERT_RTO = "threshold";
	
	private static CouchbaseEnvironment env = null;
	private static Cluster cluster = null;
	private static Bucket bucket = null;


	public static void main(String[] args) { 
		Scanner scanner = new Scanner(System.in);
		initConnection();
		welcome();
		usage();
		String cmdLn = null;
		while(!CMD_QUIT.equalsIgnoreCase(cmdLn)){
			try {
				System.out.print("# ");
				cmdLn = scanner.nextLine();
				process(cmdLn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		closeConnection();
		scanner.close();
	}

	private static void initConnection(){
		String clusterAddress = System.getProperty("cbworkshop.clusteraddress");
		String user = System.getProperty("cbworkshop.user");
		String password = System.getProperty("cbworkshop.password");
		String bucketName = System.getProperty("cbworkshop.bucket");

		try {
			//TODO: Lab 13: setup keystore and crypto

			//TODO: Lab 14: create a tracer with a KV threshold of 1us, include it in the Environment builder

			//TODO: Lab 3: create environment, connect to cluster, open bucket

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
	
	private static void closeConnection(){
		try {
			bucket.close();
			cluster.disconnect();
			env.shutdown();
		} catch (NullPointerException e) {
			System.out.println("closeConnection: no connection to close");
		}
	}

	private static void process(String cmdLn) {
		String words[] = cmdLn.split(" ");
		
		switch(words[0].toLowerCase()){
		case CMD_QUIT:
			System.out.println("bye!");
			break;
		case CMD_CREATE:
			create(words);
			break;
		case CMD_READ:
			read(words);
			break;
		case CMD_UPDATE:
			update(words);
			break;
		case CMD_SUBDOC:
			subdoc(words);
			break;
		case CMD_DELETE:
			delete(words);
			break;
		case CMD_QUERY:
			query();
			break;
		case CMD_QUERY_AIRPORTS:
			queryAirports(words);
			break;
		case CMD_BULK_WRITE:	
			bulkWrite(words);
			break;
		case CMD_BULK_WRITE_SYNC:	
			bulkWriteSync(words);
			break;
		case CMD_QUERY_ASYNC:
			queryAsync(words);
			break;
		case CMD_SEARCH:	
			search(words);
			break;
		case CMD_ENCRYPT:
			encryptPut(words);
			break;
		case CMD_ENCRYPT_GET:
			encryptGet(words);
			break;
		case CMD_UPSERT_RTO:
			upsertRTO();
			break;
		case "":
			// do nothing
			break;
		default:
			usage();					
		}
	}

	private static void create(String[] words) {
		String key = "msg::" + words[1];
		String from = words[2];
		String to = words[3];
		JsonObject json = JsonObject.create()
				.put("timestamp", System.currentTimeMillis())
			    .put("from", from)
			    .put("to", to);

		//TODO: Lab 4: add type attribute to JSON doc and insert/upsert to bucket

		System.out.println("Document created with key: " + key);	
	}

	private static void read(String[] words) {
		String key = words[1];
		
		//TODO: Lab 5: read document for the specified key from bucket into JsonDocument doc,
		//then write it to STDOUT
		
		//System.out.println(doc.content().toString());
		
	}
	
	private static void update(String[] words) {
		String key = words[1];

		//TODO: Lab 6: read the document for the key, modify attribute “name” to UPPERCASE the value, 
		//then use replace to modify the document

	}
	
	private static void delete(String[] words) {
		String key = words[1];
		
		//TODO: Lab 7: delete the document for the key
		 
	}
	
	private static void subdoc(String[] words) {
		String key = words[1];

		//TODO: Lab 8: In the document for the key, change the actual value of the “from” attribute
		//to “Administrator”, and add a new attribute: “reviewed”, with value CurrentTimeMillis

		
	}

	private static void query() {
		
		//TODO: Lab 9: Execute the query: “SELECT * FROM `travel-sample` LIMIT 10”
		//Print the results to STDOUT.  Try both raw and DSL implementations.  
		
	}

	private static void queryAirports(String[] words) {
		String sourceairport = words[1];
		String destinationairport = words[2];
		JsonObject params = JsonObject.create()
				.put("src", sourceairport)
				.put("dst", destinationairport);
		
		//TODO: Lab 10: Write a parameterized query with a join to find airlines flying from 
		//sourceairport to destinationairport, and print the results to STDOUT.

	}
	
	private static void search(String[] words) {
		String term = words[1];
	
		//TODO: Lab 12: Write code to search using the index sidx_hotel_desc for the 
		//search term and print results to STDOUT

	}
	
	private static void encryptPut(String[] words) {
		String key = words[1];
		String ccNum = words[2];
		System.out.println("Running encrypt put on field ccNum for key "+key);
		
		//TODO: Lab 13A: create a JSON doc with ccNum and timestamp fields, encrypt the ccNum field and upsert the doc

	}

	private static void encryptGet(String[] words) {
		String key = words[1];
		System.out.println("Running encrypt get on doc " + key);
	 
		//TODO: Lab 13B: get the document for the key, decrypt the content and write it to STDOUT
		
	}

	private static void upsertRTO() {
		try {
			for(int i = 0; i < 3; i++) {
				JsonDocument doc = bucket.get("airline_10");
				if (doc != null) {
				   bucket.upsert(doc);
				}
				Thread.sleep(TimeUnit.SECONDS.toMillis(1));
			}
		} catch (Exception e) {
			System.out.println(e);
		}
 
	}

	private static void bulkWrite(String[] words) {
		int size = Integer.parseInt(words[1]);
		
		System.out.println("Deleting messages ..." );

		//TODO: Lab 15A: delete all docs of type=msg from travel-sample bucket
		
		System.out.println("Writing " +size  + " messages");
		List<JsonDocument> docs = new ArrayList<JsonDocument>();
		for(int i = 0; i < size; i++){
			JsonObject json = JsonObject.create()
					.put("timestamp", System.currentTimeMillis())
				    .put("from", "me")
				    .put("to", "you")
				    .put("type", "msg");
			docs.add(JsonDocument.create("msg::" + i, json));
		}
		long ini = System.currentTimeMillis();

		//TODO: Lab 15A: asynchronously insert all of the message documents in the docs list

		System.out.println("Time elapsed for insert " + (System.currentTimeMillis() - ini) + " ms");
	}

	private static void bulkWriteSync(String[] words) {
		int size = Integer.parseInt(words[1]);
		
		System.out.println("Deleting messages ..." );

		//TODO: Lab 15B: delete all docs of type=msg from travel-sample bucket
		
		System.out.println("Writing " +size  + " messages");
		List<JsonDocument> docs = new ArrayList<JsonDocument>();
		for(int i = 0; i < size; i++){
			JsonObject json = JsonObject.create()
					.put("timestamp", System.currentTimeMillis())
				    .put("from", "me")
				    .put("to", "you")
				    .put("type", "msg");
			docs.add(JsonDocument.create("msg::" + i, json));
		}
		long ini = System.currentTimeMillis();

		//TODO: Lab 15B: synchronously insert all of the message documents in the docs list

		System.out.println("Time elapsed for insert " + (System.currentTimeMillis() - ini) + " ms");
	}
	
	private static void queryAsync(String[] words) {

		//TODO: Lab 16: Execute the query: "SELECT * FROM `travel-sample` LIMIT 5" 
		//using asynchronous implementation and print the results to STDOUT 

	}
	

private static void welcome() {
		System.out.println("Welcome to CouchbaseJavaWorkshop!");
	}
	
	private static void usage() {
		System.out.println("Usage options: \n\n" + CMD_CREATE + " [key from to] \n" + CMD_READ + " [key] \n" 
				+ CMD_UPDATE + " [airline_key] \n" + CMD_SUBDOC + " [msg_key] \n" + CMD_DELETE + " [msg_key] \n" 
				+ CMD_QUERY + " \n" + CMD_QUERY_AIRPORTS + " [sourceairport destinationairport] \n"
				+ CMD_BULK_WRITE + " [size] \n" + CMD_BULK_WRITE_SYNC + " [size] \n"
				+ CMD_QUERY_ASYNC +  " \n" + CMD_SEARCH + " [term] \n" + CMD_ENCRYPT + " [key ccNum] \n"
				+ CMD_ENCRYPT_GET + " [key] \n" +  CMD_UPSERT_RTO + " \n" + CMD_QUIT);
	}

}
