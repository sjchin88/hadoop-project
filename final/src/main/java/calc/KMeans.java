package calc;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import program.KConfig;

/**
 * Program to compute the KMeans centers 
 * @author csj
 *
 */
public class KMeans {
	public static String OUT_PREFIX = "outfile";
	public static String IN_PREFIX = "infile";
	public static final String JOB_NAME = "KMeans calc";
	public static final String SPLITTER = "\\s+";
	public static List<Centroid> centers = new ArrayList<Centroid>();
	
	
	/**
	 * Custom mapper class
	 * Read the centroids using the set up method
	 * Read the pick-up coordinate using the mapper
	 * attach the coordinate to nearest centroid and send the centroid - coordinate (key, value) pair 
	 * to the reducer
	 * @author csj
	 *
	 */
	public static class PointMapper extends TableMapper<Centroid, Coordinate>{
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
		private Centroid centerKey = new Centroid();
		private Coordinate pointValue = new Coordinate();
		private int iteration = 0;
		
		/**
		 * Set up method before each Map Task
		 * Read the centroids from the HBase
		 */
		protected void setup(Context context) throws IOException  {
			Configuration conf = context.getConfiguration();
			Connection connection = ConnectionFactory.createConnection(conf);
			iteration = Integer.valueOf(conf.get("iteration"));
			centers = readCentroidsFromHBase(connection, iteration);
			//System.out.println("Iteration:"+ iteration + " centers size:"+centers.size());
		}
		
		/**
		 * 
		 */
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double lat = Bytes.toDouble(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LATITUDE));
			double longi = Bytes.toDouble(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LONGITUDE));
			int count = Bytes.toInt(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_COUNT));

			Centroid nearest_center = new Centroid();
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
			centerKey = nearest_center;
			pointValue.setLatitude(lat);
			pointValue.setLongitude(longi);
			pointValue.setCount(count);
			context.write(centerKey, pointValue);
		}
	} // End of Mapper class
	
	public static class CenterReducer extends TableReducer<Centroid, Coordinate, ImmutableBytesWritable>{
		private Centroid center = new Centroid();
		private String rowPrefix = ""+System.nanoTime();
		private int iteration = 0;
		
		/**
		 * Set up method before each Map Task
		 * Read the centroids from the local cache file
		 */
		protected void setup(Context context) throws IOException  {
			Configuration conf = context.getConfiguration();
			iteration = Integer.valueOf(conf.get("iteration")) + 1;
			System.out.println("Iteration Reducer:"+ iteration);
		}
		
		
		/**
		 * Reduce function will calculate the new center for all points attached to the old center
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		public void reduce(Centroid key, Iterable<Coordinate> values, Context context) throws IOException, InterruptedException {
			double sumLat = 0;
			double sumLong = 0;
			int count = 0;
			//StringBuilder pointsRec = new StringBuilder();
			for(Coordinate value:values) {
				int pt_cnt = value.getCount().get();
				sumLat += value.getLatitude().get() * pt_cnt;
				sumLong += value.getLongitude().get() * pt_cnt;
				//pointsRec.append( " " + value.toString());
				count += pt_cnt;
			}
			// calculate the new center
			Double newLat = sumLat / count;
			newLat = Precision.round(newLat,4);
			Double newLong = sumLong / count;
			newLong = Precision.round(newLong, 4);
			//center.setIdx(key.getIdx());
			// Emit new center and point
			String rowKey = rowPrefix + "-"+key.getIdx().get();
	        Put record = new Put(rowKey.getBytes());
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_LATITUDE, Bytes.toBytes(newLat));
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_LONGITUDE, Bytes.toBytes(newLong));
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_IDX, Bytes.toBytes(key.getIdx().get()));
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_ITERATION, Bytes.toBytes(iteration));
			context.write(null, record);
		}
	} // End of reducer
	
	/**
	 * Main Driver method
	 * @param args	args[0] = path of the input file for centroids, args[1] = intermediate folder for output files
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
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
			// Instantiating configuration class
			Configuration config = HBaseConfiguration.create();
									        
			if(KConfig.IS_AWS) {
						//Required for AWS
				config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL());
			}
			HBaseAdmin.available(config);
			Connection connection = ConnectionFactory.createConnection(config);
			config.set("iteration", ""+iteration);
			Job job = Job.getInstance(config, JOB_NAME);
			
			//upload the file to hdfs
			//job.addCacheFile(hdfsPath.toUri());
			//System.out.print("hdfs path is " + hdfsPath.toString());
			job.setJarByClass(KMeans.class);
			job.setMapperClass(PointMapper.class);
			job.setReducerClass(CenterReducer.class);
			job.setMapOutputKeyClass(Centroid.class);
			job.setMapOutputValueClass(Coordinate.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			//job.setNumReduceTasks(1);
			//FileOutputFormat.setOutputPath(job, new Path(outputFile));
			TableMapReduceUtil.initTableMapperJob(KConfig.HTABLE_NAME, setScan(), PointMapper.class, Centroid.class, Coordinate.class, job);
			TableMapReduceUtil.initTableReducerJob(KConfig.TABLE_CENTROID, CenterReducer.class, job);
			job.waitForCompletion(true);
			
			
			if(isConverged(connection, iteration, iteration + 1)) {
				isDone = true;
			}
			
			++iteration;
		}
	} // End of main method
	
	public static boolean isConverged(Connection connection, int prevItr, int currItr) throws IOException {
		boolean isConverging = true;
		List<Centroid> centers_next = readCentroidsFromHBase(connection, currItr);
		List<Centroid> centers_prev = readCentroidsFromHBase(connection, prevItr);

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
	
	public static List<Centroid> readCentroidsFromHBase(Connection connection, int iteration) throws IOException{
		Table cTable = connection.getTable(TableName.valueOf(KConfig.TABLE_CENTROID));
		List<Centroid> centers = new ArrayList<Centroid>();
		Scan scan = setCentroidScan(iteration);
		ResultScanner rs = cTable.getScanner(scan);
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
		    // process result...
				int idx = Bytes.toInt(r.getValue(KConfig.CF_CENTROID, KConfig.COLUMN_IDX));
				double lat = Bytes.toDouble(r.getValue(KConfig.CF_CENTROID, KConfig.COLUMN_LATITUDE));
				double longi = Bytes.toDouble(r.getValue(KConfig.CF_CENTROID, KConfig.COLUMN_LONGITUDE));
				Centroid newC = new Centroid(idx, lat, longi);
				centers.add(newC);
			}
		} finally {
			rs.close();  // always close the ResultScanner!
		}
		System.out.println("Iteration:"+iteration + " centers size:"+centers.size());
		return centers;
	}
	
	/**
	 * Helper method to set the scan 
	 * @return Scan setting
	 */
	public static Scan setScan() {
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		//scan.setFilter(setFilter());
		return scan;
	}
	
	/**
	 * Helper method to set the scan 
	 * @return Scan setting
	 */
	public static Scan setCentroidScan(int iteration) {
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.setFilter(setFilter(iteration));
		return scan;
	}
	
	/**
	 * Helper method to set the filter list
	 * @return the FilterList setting
	 */
	public static FilterList setFilter(int iteration) {
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		byte[] targetItr = Bytes.toBytes(iteration);
		SingleColumnValueFilter itrFilter = new SingleColumnValueFilter(
				KConfig.CF_CENTROID, KConfig.COLUMN_ITERATION,
				CompareOperator.EQUAL, targetItr );
		filterList.addFilter(itrFilter);
		return filterList;
	}
	
}
