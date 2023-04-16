package population;

import java.io.File;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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
	public static class CentroidsReducer extends Reducer<Text, MinMaxTuple, Text, Text>{
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
			String str = "Latitude";
			context.write(new Text(str), new Text(minLat + "," + maxLat));
			str = "Longitude";
			context.write(new Text(str), new Text(minLongi + "," + maxLongi));
	    }
		
	}
	
	public static void main(String[] args) throws Throwable {
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
						        
		if(KConfig.IS_AWS) {
			//Required for AWS
			config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL());
		}
		HBaseAdmin.available(config);
		
		Job job = Job.getInstance(config, JOB_NAME);
		String outputFile = args[0];
		job.setJarByClass(MinMax.class);
		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(CentroidsReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MinMaxTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		TableMapReduceUtil.initTableMapperJob(KConfig.HTABLE_NAME, setScan(), MinMaxMapper.class, Text.class, MinMaxTuple.class, job);
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
