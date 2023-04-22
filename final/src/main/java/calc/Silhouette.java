package calc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import population.MinMax;
import population.MinMaxTuple;
import population.MinMax.MinMaxMapper;
import population.MinMax.MinMaxReducer;
import program.KConfig;

public class Silhouette {
	public static final String JOB_NAME = "Silhouette calc";
	public static final String SPLITTER = "\\s+";
	public static List<Centroid> centers = new ArrayList<Centroid>();
	
	/**
	 * Custom mapper class
	 * Read the centroids using the set up method
	 * Read the pick-up coordinate using the mapper
	 * find the silhouette score of the coordinate and emit
	 * to the reducer
	 * @author csj
	 *
	 */
	public static class SilMapper extends TableMapper<DoubleWritable, IntWritable>{
		//private Centroid centerKey = new Centroid();
		//private Coordinate pointKey = new Coordinate();
		private double sumKey = 0.0;
		private int countValue = 0;
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
			double second_distance = Double.MAX_VALUE;
			double cur_distance = 0.0;
			// Find the minimum center from a point
			for (Centroid c: centers) {
				cur_distance = Math.pow(lat - c.getLatitude().get(), 2.0) + Math.pow(longi - c.getLongitude().get(), 2.0);
				if(cur_distance < min_distance) {
					nearest_center = c;
					second_distance = min_distance;
					min_distance = cur_distance;
				} else if (cur_distance < second_distance) {
					second_distance = cur_distance;
				}
			}
			
			double silScore = (second_distance - min_distance) / second_distance;
			//Emit the point and silhouette score
			sumKey += silScore;
			countValue += count;
		}
		
		/**
		 * Emit the sum and count during clean up
		 */
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new DoubleWritable(sumKey), new IntWritable(countValue));
	    }
		
	} // End of Mapper class
	
	public static class SilReducer extends TableReducer<DoubleWritable, IntWritable, ImmutableBytesWritable>{
		//private Centroid center = new Centroid();
		//private String rowPrefix = ""+System.currentTimeMillis();
		private int kValue = 0;
		private double sumAllSil = 0.0;
		private int countAll = 0;
		
		/**
		 * Set up method before each Map Task
		 * Read the centroids from the local cache file
		 */
		protected void setup(Context context) throws IOException  {
			Configuration conf = context.getConfiguration();
			kValue = Integer.valueOf(conf.get("kValue"));
		}
		
		
		/**
		 * Reduce function will calculate the new center for all points attached to the old center
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for(IntWritable value:values) {
				int count = value.get();
				sumAllSil += key.get() ;
				countAll += count;
			}
		}
		
		/**
		 * Emit the sum and count during clean up
		 */
		public void cleanup(Context context) throws IOException, InterruptedException {
			double avgSil = sumAllSil / countAll;
			System.out.println("Avg sil for kvalue of " + kValue + " is " + avgSil);
			String rowKey = "SilScore-"+kValue;
	        Put record = new Put(rowKey.getBytes());
	        record.addColumn(KConfig.CF_SILSCORE, KConfig.COLUMN_SILSCORE, Bytes.toBytes(avgSil));
	        record.addColumn(KConfig.CF_SILSCORE, KConfig.COLUMN_K, Bytes.toBytes(kValue));
			context.write(null, record);
			//context.write(new DoubleWritable(sumKey), new IntWritable(countValue));
	    }
	} // End of reducer
	
	public static void main(String[] args) throws Throwable {
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
						        
		if(KConfig.IS_AWS) {
			//Required for AWS
			config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL());
		}
		HBaseAdmin.available(config);
		int k = Integer.parseInt(args[0]);
		int iteration = Integer.parseInt(args[1]);
		config.set("kValue", ""+k);
		config.set("iteration", ""+iteration);
		Connection connection = ConnectionFactory.createConnection(config);
		// Create new HBase table
		createHTable(connection);
		
		Job job = Job.getInstance(config, JOB_NAME);
		String outputFile = args[0];
		job.setJarByClass(Silhouette.class);
		job.setMapperClass(SilMapper.class);
		job.setReducerClass(SilReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		TableMapReduceUtil.initTableMapperJob(setScan(), SilMapper.class, DoubleWritable.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(KConfig.TABLE_SILSCORE, SilReducer.class, job);
		//FileOutputFormat.setOutputPath(job, new Path(outputFile));
		job.waitForCompletion(true);
		System.out.println("Sil Score calculated");
	}
	
	/**
	 * Helper method to set the scan 
	 * @return Scan setting
	 */
	public static List<Scan> setScan() {
		List<Scan> scans = new ArrayList<Scan>();
		for(int i = 0; i < 100; i++) {
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(KConfig.HTABLE_NAME));
			scan.setRowPrefixFilter(Bytes.toBytes(""+i));
			scans.add(scan);
		}
		
		//scan.addColumn(COLUMN_FAMILY,COLUMN_LATITUDE);
		return scans;
	}
	
	
	/**
	 * Helper method to initialize the HTable
	 * @throws IOException
	 */
	public static void createHTable(Connection connection) throws IOException {    
        // Instantiating HbaseAdmin class
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
        TableName tName = TableName.valueOf(KConfig.TABLE_SILSCORE);
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tName);
        
        ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(KConfig.CF_SILSCORE);
        // Adding column families to table descriptor
        tdb.setColumnFamily(cfd.build());
        
        // Create HTable if not exist, and clear the previous table and recreate if table already exist
        if (!admin.tableExists(tName)) {
            TableDescriptor desc = tdb.build();
            admin.createTable(desc);
            System.out.println(" Table created ");
        }
        else {
            System.out.println(" Table already exists ");
        }
	} // end of createHTable() method
	
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
