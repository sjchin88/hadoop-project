package calc;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class KMeans {
	public static String OUT_PREFIX = "outfile";
	public static String IN_PREFIX = "infile";
	public static final String CENTROID_FILE = "/centroids.txt";
	public static final String OUT_FILE = "/part-r-00000";
	public static final String DATA_FILE = "/sample_small.csv";
	public static final String JOB_NAME = "KMeans calc";
	public static final String SPLITTER = "\\s+";
	public static List<Centroid> centers = new ArrayList<Centroid>();
	
	public static class PointMapper extends Mapper<LongWritable, Text, Centroid, Coordinate>{
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
		private Centroid centerKey = new Centroid();
		private Coordinate pointValue = new Coordinate();
		
		/**
		 * Set up method before each Map Task
		 * Initialize the csvParser and get the BufferedMutator connection
		 */
		protected void setup(Context context) throws IOException  {
			Configuration conf = context.getConfiguration();
			URI[] localCacheFiles = context.getCacheFiles();
			readCentroids(localCacheFiles[0]);
			
		}
		
		/**
		 * Helper function to read the centroids from the cacheFile
		 * @param cacheFileURI
		 * @throws NumberFormatException
		 * @throws IOException
		 */
		public static void readCentroids(URI cacheFileURI) throws NumberFormatException, IOException {
			centers.clear();
			BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFileURI.getPath()));
			String line;
			try {
				// Read the centroids from the file, split by the splitter and store it into the list
				while((line = cacheReader.readLine()) != null) {
					System.out.println(line);
					String[] tokens = line.trim().split(SPLITTER);
					String[] temps = tokens[0].split(",");
					System.out.println(temps[0] + ", " + temps[1]+ ", " + temps[2]);
					int idx = Integer.parseInt(temps[0]);
					double lat = Double.parseDouble(temps[1]);
					double longi = Double.parseDouble(temps[2]);
					Centroid newC = new Centroid(idx, lat, longi);
					centers.add(newC);
				}
			} finally {
				cacheReader.close();
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] records = this.csvParser.parseLine(line);
			double lat = Double.parseDouble(records[1]);
			double longi = Double.parseDouble(records[2]);
			System.out.println(lat + "," + longi);
			Centroid nearest_center = centers.get(0);
			double min_distance = Double.MAX_VALUE;
			double cur_distance = 0.0;
			// Find the minimum center from a point
			for (Centroid c: centers) {
				cur_distance = Math.pow(lat - c.getLatitude().get(), 2.0) + Math.pow(longi - c.getLongitude().get(), 2.0);
				if(cur_distance < min_distance) {
					nearest_center = c;
					min_distance = cur_distance;
				}
			}
			
			//Emit the nearest center and the point
			//System.out.println("nearest: " + nearest_center + " point:" + point);
			centerKey = nearest_center;
			pointValue.setLatitude(lat);
			pointValue.setLongitude(longi);
			pointValue.setCount(1);
			context.write(centerKey, pointValue);
		}
	} // End of Mapper class
	
	public static class CenterReducer extends Reducer<Centroid, Coordinate, Text, Text>{
		private Centroid center = new Centroid();
		
		/**
		 * Reduce function will calculate the new center for all points attached to the old center
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		public void reduce(Centroid key, Iterable<Coordinate> values, Context context) throws IOException, InterruptedException {
			double sumLat = 0;
			double sumLong = 0;
			int count = 0;
			StringBuilder pointsRec = new StringBuilder();
			for(Coordinate value:values) {
				int pt_cnt = value.getCount().get();
				sumLat += value.getLatitude().get() * pt_cnt;
				sumLong += value.getLongitude().get() * pt_cnt;
				pointsRec.append( " " + value.toString());
				count += pt_cnt;
			}
			// calculate the new center
			Double newLat = sumLat / count;
			newLat = Precision.round(newLat,4);
			Double newLong = sumLong / count;
			newLong = Precision.round(newLong, 4);
			center.setIdx(key.getIdx());
			center.setLatitude(newLat);
			center.setLongitude(newLong);
			// Emit new center and point
			context.write(new Text(center.toString()), new Text(pointsRec.toString()));
		}
	} // End of reducer
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		IN_PREFIX = args[0];
		OUT_PREFIX = args[1];
		int max_limit = Integer.parseInt(args[2]);
		
		String inputFile = IN_PREFIX;
		String outputFile = OUT_PREFIX + System.nanoTime();
		String reinputFile = outputFile;
		
		// Reiterate until convergence or max_limit reach
		int iteration = 0;
		boolean isDone = false;
		while(!isDone && iteration < max_limit) {
			System.out.println("Iterations:"+iteration + " max limit:" + max_limit);
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, JOB_NAME);
			Path hdfsPath = new Path(inputFile + CENTROID_FILE);
			if(iteration != 0) {
				hdfsPath = new Path(reinputFile + OUT_FILE);
			} 
			//upload the file to hdfs
			job.addCacheFile(hdfsPath.toUri());
			System.out.print("hdfs path is " + hdfsPath.toString());
			
			job.setJarByClass(KMeans.class);
			job.setMapperClass(PointMapper.class);
			job.setReducerClass(CenterReducer.class);
			job.setMapOutputKeyClass(Centroid.class);
			job.setMapOutputValueClass(Coordinate.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputFile + DATA_FILE));
			FileOutputFormat.setOutputPath(job, new Path(outputFile));
			job.waitForCompletion(true);
			
			// Check for variation
			String prev = inputFile + CENTROID_FILE;
			if (iteration != 0) {
				prev = reinputFile + OUT_FILE;
			};
			
			if(isConverged(outputFile, prev)) {
				isDone = true;
			}
			
			++iteration;
			reinputFile = outputFile;
			outputFile = OUT_PREFIX + System.nanoTime();
		}
	} // End of main method
	
	public static boolean isConverged(String outputFile, String prev) throws IOException {
		boolean isConverging = true;
		Path ofile = new Path(outputFile + OUT_FILE);
		List<Centroid> centers_next = readCentroidsFromFile(ofile);
		
		Path prevfile = new Path(prev);
		List<Centroid> centers_prev = readCentroidsFromFile(prevfile);

		// Sort the old centroid and new centroid and check for convergence
		// condition
		Collections.sort(centers_next);
		Collections.sort(centers_prev);

		Iterator<Centroid> it = centers_prev.iterator();
		for (Centroid d : centers_next) {
			Centroid temp = it.next();
			if (Math.abs(temp.getLatitude().get() - d.getLatitude().get()) > 0.0001 || 
					Math.abs(temp.getLongitude().get() - d.getLongitude().get()) > 0.0001) {
				isConverging = false;
				break;
			}
		}
		return isConverging;
	}
	
	public static List<Centroid> readCentroidsFromFile(Path filePath) throws IOException{
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(filePath)));
		List<Centroid> centers = new ArrayList<Centroid>();
		String line;
		while ((line = br.readLine()) != null) {
			String[] tokens = line.trim().split(SPLITTER);
			String[] temps = tokens[0].split(",");
			//System.out.println(temps[0] + ", " + temps[1]+ ", " + temps[1]);
			int idx = Integer.parseInt(temps[0]);
			double lat = Double.parseDouble(temps[1]);
			double longi = Double.parseDouble(temps[2]);
			Centroid newC = new Centroid(idx, lat, longi);
			centers.add(newC);
		}
		br.close();
		return centers;
	}
	
	
}