package cluster.runner;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.MongoClient;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.bson.Document;
// Volume: Find the total volume for the station Foster NB for Sept 21, 2011.
public class Query2 {
	public static void main(String[] args) {
		MongoClient mongo = new MongoClient("localhost", 27017);
		MongoDatabase database = mongo.getDatabase("mongodb");
		Instant startinstance = Instant.now();
		MongoCollection<Document> collection = database.getCollection("highways");
		FindIterable<Document> highways = collection.find();
		ArrayList<String> detectorids = new ArrayList<String>();
		for(Document highway : highways) {
			ArrayList<Document> arr = (ArrayList<Document>) highway.get("stations");
			for(Document station : arr) {
				if(String.valueOf(station.get("locationtext")).equalsIgnoreCase("Foster NB")) {
					ArrayList<Document> arr1 = (ArrayList<Document>) station.get("detectors");
					for(Document detector : arr1) {
						detectorids.add(String.valueOf(detector.get("detectorid")));
					}
				}
			}
		}
		Set<Integer> tokens = new HashSet<Integer>();
		for(String s : detectorids) {
			tokens.add(Integer.parseInt(s));
		}
		System.out.println("Detector id's: "+Arrays.toString(detectorids.toArray()));
		
		/*
		 *  PART 2 : Now query the loopdata to find the detectors
		 */
		
		MongoCollection<Document> loopcollection = database.getCollection("loopdata1");
		int sum = 0;
		Date start = null, end = null;
		try {
			start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-21 00:00:00");
			end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 00:00:00");
			System.out.println("Starttime:"+start+" and Endtime:"+end);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}  
		FindIterable<Document> loopdata = loopcollection.find(Filters
				.and(Filters.in("detectorid", tokens), Filters.gte("starttime", start.getTime()), 
						Filters.lt("starttime", end.getTime())));
		int count = 0;
		for(Document loop : loopdata) {
			count++;
			if(loop.get("volume") != null) {
				sum += (Integer)loop.get("volume");
			}
		}
		System.out.println("No of matching records:"+count);
		System.out.println("Sum of volumes: "+sum);
		Instant finishinstance = Instant.now();
		long timeElapsed = Duration.between(startinstance, finishinstance).toMillis();  //in millis
		System.out.println("time taken:"+timeElapsed);
		// close db connection
		mongo.close();

	}
}
