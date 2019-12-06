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
import java.util.Scanner;
import java.util.Set;

import org.bson.Document;

//Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on 
//September 22, 2011 for the I-205 NB freeway. Report travel time in minutes.
public class Query5 {
	public static void main(String[] args) {

		MongoClient mongo = new MongoClient("localhost", 27017);
		MongoDatabase database = mongo.getDatabase("mongodb");
		Instant startinstance = Instant.now();
		MongoCollection<Document> collection = database.getCollection("highways");
//		DBCollection collection = database.getCollection("highways");
		FindIterable<Document> highways = collection.find();
		ArrayList<String> detectorids = new ArrayList<String>();
		double stationLength = 0;
		for (Document highway : highways) {
			if (String.valueOf(highway.get("highwayname")).equalsIgnoreCase("I-205")
					&& String.valueOf(highway.get("shortdirection")).equalsIgnoreCase("N")) {
				ArrayList<Document> arr = (ArrayList<Document>) highway.get("stations");
				for (Document station : arr) {
						stationLength += Double.parseDouble((String) station.get("length"));
						ArrayList<Document> arr1 = (ArrayList<Document>) station.get("detectors");
						for (Document detector : arr1) {
							detectorids.add(String.valueOf(detector.get("detectorid")));
						}
				}
			}
		}
		Set<Integer> tokens = new HashSet<Integer>();
		for (String s : detectorids) {
			tokens.add(Integer.parseInt(s));
		}
		System.out.println("Detector id's that belongs to I-205 NB freeway:");
		System.out.println(Arrays.toString(detectorids.toArray()));

		/*
		 * PART 2 : Now query the loopdata to find the detectors
		 */

		MongoCollection<Document> loopcollection = database.getCollection("loopdata1");
		int sum = 0;
		// System.out.println("Enter the date range in the given format : yyyy-MM-dd
		// HH:mm:ss");
		// System.out.println("Enter the start range :");
		// Scanner input1 = new Scanner(System.in);
		// String dateinput1 = input1.nextLine();
		// System.out.println("Enter the end range :");
		// Scanner input2 = new Scanner(System.in);
		// String dateinput2 = input2.nextLine();
		Date startDate1 = null;
		Date endDate1 = null;
		Date startDate2 = null;
		Date endDate2 = null;
		try {
			// startDate = new SimpleDateFormat("yyyy-MM-dd
			// HH:mm:ss").parse(dateinput1.trim());
			// endDate = new SimpleDateFormat("yyyy-MM-dd
			// HH:mm:ss").parse(dateinput2.trim());
			startDate1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 07:00:00");
			endDate1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 09:00:00");
			startDate2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 16:00:00");
			endDate2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 18:00:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FindIterable<Document> loopdata1 = loopcollection.find(Filters.and(Filters.in("detectorid", tokens),
				Filters.gte("starttime", startDate1.getTime()), Filters.lte("starttime", endDate1.getTime())));
		FindIterable<Document> loopdata2 = loopcollection.find(Filters.and(Filters.in("detectorid", tokens),
				Filters.gte("starttime", startDate2.getTime()), Filters.lte("starttime", endDate2.getTime())));
		long count = 0;
		for (Document loop : loopdata1) {
			if (loop.get("speed") != null) {
				if ((Integer) loop.get("volume") > 0) {
					sum += (Integer) loop.get("speed");
					count++;
				}
			}
		}
		for (Document loop : loopdata2) {
			if (loop.get("speed") != null) {
				if ((Integer) loop.get("volume") > 0) {
					sum += (Integer) loop.get("speed");
					count++;
				}
			}
		}
		System.out.println("Sum of speeds: " + sum);
		System.out.println("Count: " + count);
		double avg = (double) sum / count;
		System.out.println("Average speed: " + avg);
		System.out.println("Length of all staion in I-205 NB Freeway: " + stationLength);
		System.out.println("The average travel time for 7-9AM and 4-6PM on September 22, 2011 for the I-205 NB freeway in minutes: " + (stationLength / avg) * 60);
		Instant finishinstance = Instant.now();
		long timeElapsed = Duration.between(startinstance, finishinstance).toMillis();  //in millis
		System.out.println("time taken:"+timeElapsed);
		// close db connection
		mongo.close();
	}
}
