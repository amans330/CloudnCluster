package cluster.runner;

import java.time.Duration;
import java.time.Instant;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
// Count high speeds: Find the number of speeds > 100 in the data set.
public class Query1 {
	public static void main(String[] args) {
		MongoClient mongo = new MongoClient("localhost", 27017);
		MongoDatabase database = mongo.getDatabase("mongodb");
		Instant startinstance = Instant.now();
		MongoCollection<Document> loopcollection = database.getCollection("loopdata1");
		long result = loopcollection.countDocuments(Filters.gt("speed", 100));
//		FindIterable<Document> loopdata = loopcollection.find(Filters.gt("speed",100));
		Instant finishinstance = Instant.now();
		long timeElapsed = Duration.between(startinstance, finishinstance).toMillis();  //in millis
		System.out.println("Time taken:"+timeElapsed);
		System.out.println("The number of speeds > 100 in the data set:"+result);
		mongo.close();
	}
}
