import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

@SuppressWarnings("deprecation")
public class KMeans {
	public static String OUT_PREFIX = "outfile";
	public static String IN_PREFIX = "infile";
	public static final String CENTROID_FILE = "/centroids.txt";
	public static final String OUT_FILE = "/part-r-00000";
	public static final String DATA_FILE = "/data.txt";
	public static final String JOB_NAME = "KMeans calc";
	public static final String SPLITTER = "\\s+";
	public static List<Double> centers = new ArrayList<Double>();
	
	public static class PointMapper extends Mapper<LongWritable, Text, DoubleWritable, DoubleWritable>{
		/**
		 * Set up method before each Map Task
		 * Initialize the csvParser and get the BufferedMutator connection
		 */
		protected void setup(Context context) throws IOException  {
			Configuration conf = context.getConfiguration();
			URI[] localCacheFiles = context.getCacheFiles();
			readCentroids(localCacheFiles[0]);
			
		}
		
		public static void readCentroids(URI cacheFileURI) throws NumberFormatException, IOException {
			centers.clear();
			BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFileURI.getPath()));
			String line;
			try {
				// Read the centroids from the file, split by the splitter and store it into the list
				while((line = cacheReader.readLine()) != null) {
					System.out.println(line);
					String[] temps = line.trim().split(SPLITTER);
					System.out.println(temps[0]);
					centers.add(Double.parseDouble(temps[0]));
				}
			} finally {
				cacheReader.close();
			}
				
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			double point = Double.parseDouble(line);
			double nearest_center = centers.get(0);
			double min1 = Double.MAX_VALUE;
			double min2 = nearest_center;
			// Find the minimum center from a point
			for (double c: centers) {
				min1 = c - point;
				if(Math.abs(min1) < Math.abs(min2)) {
					nearest_center = c;
					min2 = min1;
				}
			}
			//Emit the nearest center and the point
			System.out.println("nearest: " + nearest_center + " point:" + point);
			context.write(new DoubleWritable(nearest_center), new DoubleWritable(point));
		}
	} // End of Mapper class
	
	public static class CenterReducer extends Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text>{
		
		/**
		 * Reduce function will calculate the new center for all points attached to the old center
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double newCenter = 0.0;
			double sum = 0;
			int no_points = 0;
			String points = "";
			for(DoubleWritable value:values) {
				double d = value.get();
				points = points + " " + Double.toString(d);
				sum += d;
				no_points++;
			}
			// calculate the new center
			newCenter = sum / no_points;
			
			// Emit new center and point
			context.write(new DoubleWritable(newCenter), new Text(points));
		}
	} // End of reducer
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		IN_PREFIX = args[0];
		OUT_PREFIX = args[1];
		int max_limit = Integer.parseInt(args[2]);
		
		String inputFile = IN_PREFIX;
		String outputFile = OUT_PREFIX + System.nanoTime();
		String reinputFile = outputFile;
		
		// Reiterate untill convergence or max_limit reach
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
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(inputFile + DATA_FILE));
			FileOutputFormat.setOutputPath(job, new Path(outputFile));
			job.waitForCompletion(true);
			
			Path ofile = new Path(outputFile + OUT_FILE);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));
			List<Double> centers_next = new ArrayList<Double>();
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split("\t| ");
				double c = Double.parseDouble(sp[0]);
				centers_next.add(c);
				line = br.readLine();
			}
			br.close();

			String prev;
			if (iteration == 0) {
				prev = inputFile + CENTROID_FILE;
			} else {
				prev = reinputFile + OUT_FILE;
			}
			Path prevfile = new Path(prev);
			FileSystem fs1 = FileSystem.get(new Configuration());
			BufferedReader br1 = new BufferedReader(new InputStreamReader(
					fs1.open(prevfile)));
			List<Double> centers_prev = new ArrayList<Double>();
			String l = br1.readLine();
			while (l != null) {
				String[] sp1 = l.split(SPLITTER);
				double d = Double.parseDouble(sp1[0]);
				centers_prev.add(d);
				l = br1.readLine();
			}
			br1.close();

			// Sort the old centroid and new centroid and check for convergence
			// condition
			Collections.sort(centers_next);
			Collections.sort(centers_prev);

			Iterator<Double> it = centers_prev.iterator();
			for (double d : centers_next) {
				double temp = it.next();
				if (Math.abs(temp - d) <= 0.1) {
					isDone = true;
				} else {
					isDone = false;
					break;
				}
			}
			++iteration;
			reinputFile = outputFile;
			outputFile = OUT_PREFIX + System.nanoTime();
		}
	}
	
	
}
