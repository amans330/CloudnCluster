package cluster.runner;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

public class App {
	public static void main(String[] args) {
		/**** Read from CSV ****/
		String highwayFile = "C:/Users/Sunny/Downloads/highways.csv";
		String stationsFile = "C:/Users/Sunny/Downloads/freeway_stations.csv";
		String detectorsFile = "C:/Users/Sunny/Downloads/freeway_detectors.csv";
		String loopDataFile = "C:/Users/Sunny/Downloads/freeway_loopdata.csv";
		BufferedReader brh = null;
		BufferedReader brs = null;
		BufferedReader brd = null;
		BufferedReader br = null;
		String hl = "";
		String sl = "";
		String dl = "";
		String loop = "";
		String cvsSplitBy = ",";

		// Creating a Mongo client
		MongoClient mongo = new MongoClient("localhost", 27017);
		// Creating Credentials
		MongoCredential credential;
		credential = MongoCredential.createCredential("sampleUser", "myDb", "password".toCharArray());
		System.out.println("Connected to the database successfully");
		// Accessing the database
		MongoDatabase database = mongo.getDatabase("mongodb");

		// Retrieving collection highways
		MongoCollection<Document> collection = database.getCollection("highways");
//		collection.deleteMany(new Document());
		// read CSV data
		try {
			brh = new BufferedReader(new FileReader(highwayFile));
			Document highway_document;
			Document station_document;
			Document detector_document;
			Document loop_document;

			while ((hl = brh.readLine()) != null) {
				List<Document> stationdocuments = new ArrayList<Document>();
				String[] highway = hl.split(cvsSplitBy);
				highway_document = new Document("highwayid", highway[0]).append("shortdirection", highway[1])
						.append("direction", highway[2]).append("highwayname", highway[3]);
				String highwayid = highway[0];

				brs = new BufferedReader(new FileReader(stationsFile));
				while ((sl = brs.readLine()) != null) {
					String[] station = sl.split(cvsSplitBy);
					String stationid = station[0];
					if (station[1] != null && !station[1].isEmpty() && station[1].equalsIgnoreCase(highwayid)) {
						station_document = new Document("stationid", station[0]).append("highwayid", station[1])
								.append("milepost", station[2]).append("locationtext", station[3])
								.append("upstream", station[4]).append("downstream", station[5])
								.append("stationclass", station[6]).append("numberlanes", station[7])
								.append("latlon", station[8] + "," + station[9]).append("length", station[10]);

						brd = new BufferedReader(new FileReader(detectorsFile));
						List<Document> detectordocuments = new ArrayList<Document>();
						while ((dl = brd.readLine()) != null) {
							String[] detector = dl.split(cvsSplitBy);
							if (detector[6] != null && !detector[6].isEmpty()
									&& detector[6].equalsIgnoreCase(stationid)) {
								detector_document = new Document("detectorid", detector[0])
										.append("highwayid", detector[1]).append("milepost", detector[2])
										.append("locationtext", detector[3]).append("detectorclass", detector[4])
										.append("lanenumber", detector[5]).append("stationid", detector[6]);
								detectordocuments.add(detector_document);
							}
						}
						station_document.append("detectors", detectordocuments);
						stationdocuments.add(station_document);
					}

				}
				highway_document.append("stations", stationdocuments);
				collection.insertOne(highway_document);
			}
			

			// Retrieving collection loop collection
			MongoCollection<Document> loopCollection = database.getCollection("loopdata1");
			br = new BufferedReader(new FileReader(loopDataFile));
//			int count = 0;
			Date startDate = new Date();
			SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			while ((loop = br.readLine()) != null) {
				String[] loop_record = loop.split(cvsSplitBy);
				try {
					startDate = sd.parse(loop_record[1].substring(0,18));
//					System.out.println(startDate.getTime());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				loop_document = new Document("detectorid", loop_record[0] != null ? Integer.parseInt(loop_record[0]) : null)
						.append("starttime", startDate.getTime())
						.append("volume", loop_record[2].equals("") ? null : Integer.parseInt(loop_record[2]))
						.append("speed", loop_record[3].equals("") ? null : Integer.parseInt(loop_record[3]))
						.append("occupany", loop_record[4].equals("") ? null : Integer.parseInt(loop_record[4]))
						.append("status", loop_record[5].equals("") ? "" : Integer.parseInt(loop_record[5]))
						.append("dqflags", loop_record[6].equals("") ? "" : Integer.parseInt(loop_record[6]));
				loopCollection.insertOne(loop_document);
			}

//			loopCollection.deleteMany(Filters.or(Filters.lt("speed", 5), 
//					Filters.and(Filters.eq("volume", 0), Filters.gt("speed", 0))));
			
			System.out.println("Execution finished!!");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (brh != null) {
				try {
					brh.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (brs != null) {
				try {
					brs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (brd != null) {
				try {
					brd.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

//	private static void getAllDocuments(MongoCollection<Document> col) {
//		FindIterable<Document> fi = col.find();
//		MongoCursor<Document> cursor = fi.iterator();
//		try {
//			while (cursor.hasNext()) {
//				System.out.println(cursor.next().toJson());
//			}
//		} finally {
//			cursor.close();
//		}
//	}
}
