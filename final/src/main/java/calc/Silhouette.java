package calc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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

import org.apache.hadoop.mapreduce.Job;

import program.KConfig;

/**
 * Program to calculate the silhouette score
 * @author csj
 *
 */
public class Silhouette {
	public static final String JOB_NAME = "Silhouette calc";
	public static List<Centroid> centers = new ArrayList<Centroid>();
	
	/**
	 * Custom mapper class
	 * Read the centroids using the set up method
	 * Read the pick-up coordinate using the mapper
	 * Calculate the silhouette score of the coordinate and emit
	 * to the reducer
	 * @author csj
	 *
	 */
	public static class SilMapper extends TableMapper<DoubleWritable, IntWritable>{
		private double sumKey = 0.0;
		private int countValue = 0;
		private int iteration = 0;
		private int kValue;
		
		/**
		 * Set up method before each Map Task
		 * Read the centroids from the HBase
		 */
		protected void setup(Context context) throws IOException  {
			Configuration conf = context.getConfiguration();
			Connection connection = ConnectionFactory.createConnection(conf);
			iteration = Integer.valueOf(conf.get("iteration"));
			kValue = Integer.valueOf(conf.get("kValue"));
			centers = KMeans.readCentroidsFromHBase(connection, iteration, kValue);
			//System.out.println("Iteration:"+ iteration + " centers size:"+centers.size());
		}
		
		/**
		 * Calculate the silScore for each coordinate points
		 */
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			double lat = Bytes.toDouble(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LATITUDE));
			double longi = Bytes.toDouble(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LONGITUDE));
			int count = Bytes.toInt(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_COUNT));

			double min_distance = Double.MAX_VALUE;
			double second_distance = Double.MAX_VALUE;
			double cur_distance = 0.0;
			// Find the minimum center from a point
			for (Centroid c: centers) {
				cur_distance = Math.pow(lat - c.getLatitude().get(), 2.0) + Math.pow(longi - c.getLongitude().get(), 2.0);
				if(cur_distance < min_distance) {
					second_distance = min_distance;
					min_distance = cur_distance;
				} else if (cur_distance < second_distance) {
					second_distance = cur_distance;
				}
			}
			
			double silScore = (second_distance - min_distance) / second_distance;
			//Emit the point and silhouette score
			sumKey += silScore * count;
			countValue += count;
		}
		
		/**
		 * Emit the sum and count during clean up
		 */
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new DoubleWritable(sumKey), new IntWritable(countValue));
	    }
		
	} // End of Mapper class
	
	/**
	 * Reducer class, compute silscore for all points 
	 * @author csj
	 *
	 */
	public static class SilReducer extends TableReducer<DoubleWritable, IntWritable, ImmutableBytesWritable>{
		private int kValue = 0;
		private int iteration = 0;
		private double sumAllSil = 0.0;
		private int countAll = 0;
		
		/**
		 * Set up method before each Reducer Task
		 */
		protected void setup(Context context) throws IOException  {
			Configuration conf = context.getConfiguration();
			iteration = Integer.valueOf(conf.get("iteration"));
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
	        record.addColumn(KConfig.CF_SILSCORE, KConfig.COLUMN_ITERATION, Bytes.toBytes(iteration));
			context.write(null, record);
	    }
	} // End of reducer
	
	/**
	 * Driver method
	 * @param args args[0] current k value, args[1] last iteration value
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
		int jobNums = Integer.valueOf(args[2]);
		if(KConfig.IS_AWS) {
			//Required for AWS
			config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL());
		}
		HBaseAdmin.available(config);
		int kValue = Integer.parseInt(args[0]);
		int iteration = Integer.parseInt(args[1]);
		config.set("kValue", ""+kValue);
		config.set("iteration", ""+iteration);
		Connection connection = ConnectionFactory.createConnection(config);
		// Create new HBase table
		createHTable(connection);
		
		Job job = Job.getInstance(config, JOB_NAME);
		job.setJarByClass(Silhouette.class);
		job.setMapperClass(SilMapper.class);
		job.setReducerClass(SilReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		TableMapReduceUtil.initTableMapperJob(setScan(jobNums), SilMapper.class, DoubleWritable.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(KConfig.TABLE_SILSCORE, SilReducer.class, job);

		job.waitForCompletion(true);
		System.out.println("Sil Score calculated");
	}
	
	/**
	 * Helper method to set the scan for points
	 * @return Scan setting
	 */
	public static List<Scan> setScan(int jobNums) {
		List<Scan> scans = new ArrayList<Scan>();
		for(int i = 0; i < jobNums; i++) {
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(KConfig.TABLE_PT));
			int prefix = i + 10;
			scan.setRowPrefixFilter(Bytes.toBytes(""+prefix));
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
	
}
