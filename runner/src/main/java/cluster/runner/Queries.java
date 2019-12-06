package cluster.runner;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;

public class Queries {
	public static void main(String[] args) {
		
		MongoClient mongo = new MongoClient("localhost", 27017);
		MongoDatabase database = mongo.getDatabase("mongodb");
		MongoCollection<Document> collection = database.getCollection("highways");
//		DBCollection collection = database.getCollection("highways");
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
		
		MongoCollection<Document> loopcollection = database.getCollection("loopdata");
		int sum = 0;
		FindIterable<Document> loopdata = loopcollection.find(Filters
				.and(Filters.in("detectorid", tokens), Filters.regex("starttime", "2011-09-21")));
		for(Document loop : loopdata) {
			if(loop.get("volume") != null) {
				sum += (Integer)loop.get("volume");
			}
		}
		System.out.println("Sum of volumes: "+sum);
		// close db connection
		mongo.close();
	}
	
}
