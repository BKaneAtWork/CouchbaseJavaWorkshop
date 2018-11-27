package com.cbworkshop;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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
import com.couchbase.client.java.search.queries.*;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;
import com.couchbase.client.java.subdoc.DocumentFragment;
import com.couchbase.client.java.error.*;
import com.couchbase.client.java.error.subdoc.*;

import rx.Observable;

import static com.couchbase.client.java.query.Select.select;

public class MainLabSolution {

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

	private static Bucket bucket = null;

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		initConnection();
		welcome();
		usage();
		String cmdLn = null;
		while(!CMD_QUIT.equalsIgnoreCase(cmdLn)) {
			try {
				System.out.print("# ");
				cmdLn = scanner.nextLine();
				process(cmdLn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		scanner.close();
	}

	private static void initConnection(){
		String clusterAddress = System.getProperty("cbworkshop.clusteraddress");
		String user = System.getProperty("cbworkshop.user");
		String password = System.getProperty("cbworkshop.password");
		String bucketName = System.getProperty("cbworkshop.bucket");

		//TODO: Lab 3: create environment, connect to cluster, open bucket
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
			.operationTracingEnabled(false)    // disable RTO/Threshold Logging for now, per https://docs.couchbase.com/java-sdk/2.7/threshold-logging.html
			.socketConnectTimeout(15000)
			.connectTimeout(15000)
			.kvTimeout(15000)
			.continuousKeepAliveEnabled(true)  // to keep connection active after FTS query, per https://issues.couchbase.com/browse/JCBC-1199
			.keepAliveInterval(10000)
			.build();

		Cluster cluster = CouchbaseCluster.create(env, clusterAddress);
		cluster.authenticate(user, password);
		bucket = cluster.openBucket(bucketName);
	}

	private static void process(String cmdLn) {
		String words[] = cmdLn.split(" ");

		switch(words[0].toLowerCase()) {
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
		json.put("type", "msg");
		try {
			bucket.insert(JsonDocument.create(key, json));
		} catch (DocumentAlreadyExistsException e) {
			System.out.println("Document '" + key + "' already exists.");
			return;
		}

//		bucket.upsert(JsonDocument.create(key, json));
		System.out.println("Document created with key: " + key);
	}

	private static void read(String[] words) {
		String key = words[1];

		//TODO: Lab 5: read document for the specified key from bucket into JsonDocument doc,
		//then write it to STDOUT
		JsonDocument doc = bucket.get(key);
		if (doc == null) {
			System.out.println("Document '" + key + "' not found.");
		} else {
			System.out.println(doc.content().toString());
		}
	}

	private static void update(String[] words) {
		String key = words[1];

		//TODO: Lab 6: read the document for the key, modify attribute “name” to UPPERCASE the value,
		//then use replace to modify the document
		JsonDocument doc = bucket.get(key);
		if (doc == null) {
			System.out.println("Document '" + key + "' not found.");
		} else {
			String name = doc.content().getString("name");
			if (name == null) {
				System.out.println("Document '" + key + "' does not contain attribute 'name'.");
			} else {
				doc.content().put("name", name.toUpperCase());
				bucket.replace(doc);
			}
		}
	}

	private static void delete(String[] words) {
		String key = words[1];

		//TODO: Lab 7: delete the document for the key
		try {
			bucket.remove(key);
		} catch (DocumentDoesNotExistException e) {
			System.out.println("Document '" + key + "' not found.");
		}
	}

	private static void subdoc(String[] words) {
		String key = words[1];

		//TODO: Lab 8: In the document for the key, change the actual value of the “from” attribute
		//to “Administrator”, and add a new attribute: “reviewed”, with value CurrentTimeMillis
		try {
			DocumentFragment<Mutation> result = bucket
				.mutateIn(key)
				.replace("from", "Administrator")
				.insert("reviewed", System.currentTimeMillis())
				.execute();
		} catch (DocumentDoesNotExistException e) {
			System.out.println("Document '" + key + "' not found.");
		} catch (MultiMutationException e) {
			System.out.println("Document '" + key + "' does not contain attribute 'from'.");
		}
	}

	private static void query() {
		//TODO: Lab 9: Execute the query: “SELECT * FROM `travel-sample` LIMIT 10”
		//Print the results to STDOUT.  Try both raw and DSL implementations.
		N1qlQueryResult queryResult;
		// Raw
		queryResult = bucket.query(N1qlQuery.simple("SELECT * FROM `travel-sample` LIMIT 10"));
		if (queryResult.finalSuccess() == false) {
			System.out.println("Raw: Query failed: " + queryResult.errors());
		} else {
			for(N1qlQueryRow row : queryResult) {
				System.out.println("Raw: " + row.value().toString());
			}
		}
		// DSL
		queryResult = bucket.query(select("*").from("`travel-sample`").limit(10));
		if (queryResult.finalSuccess() == false) {
			System.out.println("DSL: Query failed: " + queryResult.errors());
		} else {
			for(N1qlQueryRow row : queryResult) {
				System.out.println("DSL: " + row.value().toString());
			}
		}
	}

	private static void queryAirports(String[] words) {
		String sourceairport = words[1];
		String destinationairport = words[2];
		JsonObject params = JsonObject.create()
			.put("src", sourceairport)
			.put("dst", destinationairport);

		//TODO: Lab 10: Write a parameterized query with a join to find airlines flying from
		//sourceairport to destinationairport, and print the results to STDOUT.
		String queryStr = "SELECT a.name FROM `travel-sample` r JOIN `travel-sample` a " +
			"ON KEYS r.airlineid WHERE r.type='route' AND " +
			"r.sourceairport=$src AND r.destinationairport=$dst";

		N1qlQuery query = N1qlQuery.parameterized(queryStr, params);
		N1qlQueryResult queryResult = bucket.query(query);
		if (queryResult.finalSuccess() == false) {
			System.out.println("Query failed: " + queryResult.errors());
		} else {
			if (queryResult.rows().hasNext() == false) {
				System.out.println("No routes found for " + sourceairport + " --> " + destinationairport);
			} else {
				for(N1qlQueryRow row : queryResult) {
					System.out.println(row.value().toString());
				}
			}
		}
	}

	private static void bulkWrite(String[] words) {

		int size = Integer.parseInt(words[1]);

		System.out.println("Deleting messages ...");
		//TODO: Lab 11A: delete all docs of type=msg from travel-sample bucket
		bucket.query(N1qlQuery.simple("DELETE FROM `travel-sample` WHERE type='msg'"));

		System.out.println("Writing " + size + " messages");
		List<JsonDocument> docs = new ArrayList<JsonDocument>();
		for (int i = 0; i < size; i++) {
			JsonObject json = JsonObject.create()
				.put("timestamp", System.currentTimeMillis())
				.put("from", "me")
				.put("to", "you")
				.put("type", "msg");
			docs.add(JsonDocument.create("msg::" + i, json));
		}
		long ini = System.currentTimeMillis();
		//TODO: Lab 11A: asynchronously insert all of the message documents in the docs list
		Observable
			.from(docs)
			.flatMap(doc -> bucket.async().insert(doc))
			.last()
			.toBlocking()
			.single();
		System.out.println("Time elapsed " + (System.currentTimeMillis() - ini) + " ms");
	}

	private static void bulkWriteSync(String[] words) {

		int size = Integer.parseInt(words[1]);

		System.out.println("Deleting messages ...");
		//TODO: Lab 11B: delete all docs of type=msg from travel-sample bucket
		bucket.query(N1qlQuery.simple("DELETE FROM `travel-sample` WHERE type='msg'"));

		System.out.println("Writing " + size + " messages");
		List<JsonDocument> docs = new ArrayList<JsonDocument>();
		for (int i = 0; i < size; i++) {
			JsonObject json = JsonObject.create()
				.put("timestamp", System.currentTimeMillis())
				.put("from", "me")
				.put("to", "you")
				.put("type", "msg");
			docs.add(JsonDocument.create("msg::" + i, json));
		}
		long ini = System.currentTimeMillis();
		//TODO: Lab 11B: synchronously insert all of the message documents in the docs list
		for (JsonDocument doc : docs) {
			bucket.insert(doc);
		}
		System.out.println("Time elapsed for insert " + (System.currentTimeMillis() - ini) + " ms");
	}

	private static void queryAsync(String[] words) {

		//TODO: Lab 12: Execute the query: "SELECT * FROM `travel-sample` LIMIT 5" 
		//using asynchronous implementation and print the results to STDOUT 

		bucket.async()
			.query(N1qlQuery.simple(select("*").from("`travel-sample`").limit(5)))
			.subscribe(result -> {
				result.errors()
					.subscribe(
						e -> System.err.println("N1QL Error/Warning: " + e),
						runtimeError -> runtimeError.printStackTrace()
					);
				result.rows()
					.map(row -> row.value())
					.subscribe(
						rowContent -> System.out.println(rowContent),
						runtimeError -> runtimeError.printStackTrace()
					);
			});
	}

	private static void search(String[] words) {
		String term = words[1];

		//TODO: Lab 14: Write code to search using the index sidx_hotel_desc for the
		//search term and print results to STDOUT
		try {
			MatchQuery fts = SearchQuery.match(term);
			//TermQuery fts = SearchQuery.term(term).fuzziness(1);
			SearchQueryResult result = bucket.query(new SearchQuery("sidx_hotel_desc", fts));
			//SearchQueryResult result = bucket.query(new SearchQuery("sidx_hotel_desc", fts).limit(20));

			if (result.iterator().hasNext() == false) {
				System.out.println("No matches for '" + term + "'.");
			}
			for (SearchQueryRow row : result) {
			   System.out.println(row);
			}
		} catch (IndexDoesNotExistException e) {
			System.out.println(e);
		}

	}


	private static void welcome() {
		System.out.println("Welcome to CouchbaseJavaWorkshop!");
	}

	private static void usage() {
		System.out.println("Usage options: \n\n" + CMD_CREATE + " [key from to] \n" + CMD_READ + " [key] \n"
				+ CMD_UPDATE + " [airline_key] \n" + CMD_SUBDOC + " [msg_key] \n" + CMD_DELETE + " [msg_key] \n"
				+ CMD_QUERY + " \n" + CMD_QUERY_AIRPORTS + " [sourceairport destinationairport] \n"
				+ CMD_BULK_WRITE + " [size] \n" + CMD_BULK_WRITE_SYNC + " [size] \n"
				+ CMD_QUERY_ASYNC +  " \n" + CMD_SEARCH + " [term] \n"+ CMD_QUIT);
	}

}
