package population;

import java.io.File;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import program.KConfig;

/**
 * Program to get the min and max of the Latitudes and Longitudes from the pick up location
 * @author csj
 *
 */
public class MinMax {
	public static final String JOB_NAME = "Get the Min Max";
	
	/**
	 * Custom mapper, 
	 * the map task read data from HBase table and update the min max of Latitude and Longitude
	 * the cleanup method emit the min and max of latitude and longitude to the reducer
	 * @author vcsj
	 *
	 */
	public static class MinMaxMapper extends TableMapper<Text, MinMaxTuple>{
		private Double minLat = Double.MAX_VALUE;
		private Double maxLat = -1 * Double.MAX_VALUE;
		private Double minLongi = Double.MAX_VALUE;
		private Double maxLongi = -1 * Double.MAX_VALUE;
		private MinMaxTuple valueTuple = new MinMaxTuple();
		
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double lat = Bytes.toDouble(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LATITUDE));
			double longi = Bytes.toDouble(value.getValue(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LONGITUDE));
			minLat = Math.min(lat, minLat);
			maxLat = Math.max(lat, maxLat);
			minLongi = Math.min(minLongi, longi);
			maxLongi = Math.max(maxLongi, longi);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
	    	Text word = new Text("Latitude");
	    	valueTuple.setMin(minLat);
	    	valueTuple.setMax(maxLat);
	    	context.write(word, valueTuple);
	    	word.set("Longitude");
	    	valueTuple.setMin(minLongi);
	    	valueTuple.setMax(maxLongi);
	    	context.write(word, valueTuple);
	    }
	}
	
	/**
	 * Custom reducer
	 * @author vboxuser
	 *
	 */
	public static class MinMaxReducer extends TableReducer<Text, MinMaxTuple, ImmutableBytesWritable>{
		private Double minLat = Double.MAX_VALUE;
		private Double maxLat = -1 * Double.MAX_VALUE;
		private Double minLongi = Double.MAX_VALUE;
		private Double maxLongi = -1 * Double.MAX_VALUE;
		
		public void reduce(Text key, Iterable<MinMaxTuple> values, Context context) {
			if(key.toString().equals("Latitude")) {
				for(MinMaxTuple value:values) {
					minLat = Math.min(minLat, value.getMin().get());
					maxLat = Math.max(maxLat, value.getMax().get());
				}
			} else {
				for(MinMaxTuple value:values) {
					minLongi = Math.min(minLongi, value.getMin().get());
					maxLongi = Math.max(maxLongi, value.getMax().get());
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			String rowKey = "Latitude";
	        Put record = new Put(rowKey.getBytes());
	        record.addColumn(KConfig.CF_MINMAX, KConfig.COLUMN_MIN, Bytes.toBytes(minLat));
	        record.addColumn(KConfig.CF_MINMAX, KConfig.COLUMN_MAX, Bytes.toBytes(maxLat));
			context.write(null, record);
			rowKey = "Longitude";
	        Put record2 = new Put(rowKey.getBytes());
	        record2.addColumn(KConfig.CF_MINMAX, KConfig.COLUMN_MIN, Bytes.toBytes(minLongi));
	        record2.addColumn(KConfig.CF_MINMAX, KConfig.COLUMN_MAX, Bytes.toBytes(maxLongi));
			context.write(null, record2);
	    }
		
	}
	
	/**
	 * Helper method to initialize the HTable
	 * @throws IOException
	 */
	public static void createHTable(Connection connection) throws IOException {    
        // Instantiating HbaseAdmin class
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
        TableName tName = TableName.valueOf(KConfig.TABLE_MINMAX);
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tName);
        
        ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(KConfig.CF_MINMAX);
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
            admin.disableTable(tName);
            admin.deleteTable(tName);
            TableDescriptor desc = tdb.build();
            admin.createTable(desc);
            System.out.println(" Table recreated ");
        }
	} // end of createHTable() method
	
	public static void main(String[] args) throws Throwable {
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
						        
		if(KConfig.IS_AWS) {
			//Required for AWS
			config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL());
		}
		HBaseAdmin.available(config);
		Connection connection = ConnectionFactory.createConnection(config);
		// Create new HBase table
		createHTable(connection);
		
		Job job = Job.getInstance(config, JOB_NAME);
		String outputFile = args[0];
		job.setJarByClass(MinMax.class);
		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(MinMaxReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MinMaxTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		TableMapReduceUtil.initTableMapperJob(KConfig.HTABLE_NAME, setScan(), MinMaxMapper.class, Text.class, MinMaxTuple.class, job);
		TableMapReduceUtil.initTableReducerJob(KConfig.TABLE_MINMAX, MinMaxReducer.class, job);
		FileOutputFormat.setOutputPath(job, new Path(outputFile));
		job.waitForCompletion(true);
		System.out.println("Min Max extracted");
	}
	
	/**
	 * Helper method to set the scan 
	 * @return Scan setting
	 */
	public static Scan setScan() {
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		//scan.addColumn(COLUMN_FAMILY,COLUMN_LATITUDE);
		return scan;
	}
	
}
