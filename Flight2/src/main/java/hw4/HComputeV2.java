package hw4;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Program to compute the avg delay for each airline for each month
 * Decode based on rowKeys format: AirlineID-Year-Month-DateOfMonth-FlightNum-Origin
 * @author csj
 *
 */
public class HComputeV2 {
	private static final String HTABLE_NAME = "FlightData";
	private static final byte[] COLUMN_FAMILY = "Flight".getBytes();
	private static final byte[] COLUMN_YEAR = "yr".getBytes(); 
	private static final byte[] COLUMN_CANCELLED = "cancel".getBytes(); 
	private static final byte[] COLUMN_DELAY = "delay".getBytes(); 
	private static final byte[] TARGET_YEAR = Bytes.toBytes(2008);
	private static final byte[] NOT_CANCELLED = Bytes.toBytes(false);
	
	/**
	 * Mapper class to convert the rows fetched into the composite keys and delay value
	 * @author csj
	 *
	 */
	public static class HComputeMapper extends TableMapper<AirlineMonthPair, IntWritable> {
		private AirlineMonthPair outKey = new AirlineMonthPair();
		private IntWritable outValue = new IntWritable();
		
		/**
		 * Custom map method
		 */
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			//rowKeys format: AirlineID-Year-Month-DateOfMonth-FlightNum-Origin
			String[] rowKeys = new String(value.getRow()).split("-");
			outKey.setAirline(rowKeys[0]);
			outKey.setMonth(rowKeys[2]);
			int delay = Bytes.toInt(value.getValue(COLUMN_FAMILY, COLUMN_DELAY));
			outValue.set(delay);
			context.write(outKey, outValue);
		}
	}
	
	/**
	 * Custom Partitioner class to partition based on the airline only from the composite key
	 * @author csj
	 *
	 */
	public static class HComPartitioner extends Partitioner<AirlineMonthPair, IntWritable>{

		@Override
		public int getPartition(AirlineMonthPair key, IntWritable value, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.getAirline().hashCode() * 127) % numPartitions;
		}
		
	}
	
	/**
	 * Custom Key Comparator class to compare key based on airline for primary sort
	 * and the month for secondary sort
	 * @author csj
	 *
	 */
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(AirlineMonthPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			AirlineMonthPair a1 = (AirlineMonthPair) w1;
			AirlineMonthPair a2 = (AirlineMonthPair) w2;
			int cmp = a1.getAirline().compareTo(a2.getAirline());
			if(cmp != 0) {
				return cmp;
			}
			int month1 = a1.getMonth().get();
			int month2 = a2.getMonth().get();
			if (month1 == month2) {
				return 0;
			} else if (month1 < month2) {
				return -1;
			} else {
				return 1;
			}
		}
	}
	
	/**
	 * Custom GroupComparator to group key based on airline value only from the
	 * composite key
	 * @author csj
	 *
	 */
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(AirlineMonthPair.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			AirlineMonthPair a1 = (AirlineMonthPair) w1;
			AirlineMonthPair a2 = (AirlineMonthPair) w2;
			return a1.getAirline().compareTo(a2.getAirline());
		}
	}
	
	/**
	 * Reducer class 
	 * @author csj
	 *
	 */
	public static class HComputeReducer extends Reducer<AirlineMonthPair, IntWritable, Text, Text> {
		
		/*
		 * Reduce method, iterate through the IntWritable and add to the count and total Delay 
		 */
		public void reduce(AirlineMonthPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//Initialize count, totalDelay, and currMonth
			int totalDelay = 0;
			int count = 0;
			int currMonth = 1;
			
			//Initialize the output string
			StringBuilder outValues = new StringBuilder();
			outValues.append(key.getAirline().toString());
			
			//Iterate through the values and add to count and totalDelay
			for (IntWritable value: values) {
				int flightMonth = key.getMonth().get();
				//while currMonth not equal to flightMonth from the record
				while(currMonth != flightMonth) {	
					// append current month's avg value to the output and reset the count, totalDelay and 
					// increment the currMonth			
					this.appendAvg(count, totalDelay, currMonth, outValues);
					totalDelay = 0;
					count = 0;
					currMonth++;
				}
				totalDelay += value.get();
				count++;
			}
			
			// Last month's average
			this.appendAvg(count, totalDelay, currMonth, outValues);
			
			// append 0 for months with no data
			while(currMonth < 12) {
				currMonth++;
				this.appendAvg(0, 0, currMonth, outValues);
			}
			
			// Final output
			context.write(new Text(outValues.toString()), new Text());
		} // End of reduce method
		
		/**
		 * Helper function to append the average for the month to output StringBuilder
		 * @param count			count of flight for the month
		 * @param totalDelay	sum of all delay in minutes for the month
		 * @param currMonth		month
		 * @param sb			output StringBuilder object
		 */
		public void appendAvg(int count, int totalDelay, int currMonth, StringBuilder sb) {
			if(count == 0) {
				sb.append(",(" + currMonth + ",0)");
			} else {
				//Compute previous month's average and append to the output string
				int average = (int) Math.ceil(totalDelay * 1.0 / count);
				sb.append(",(" + currMonth + "," + average + ")");
			}
		}
	} // End of HComputeReducer
	
	/**
	 * Driver method
	 * @param args input output passed through from CLI
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
						        
		// Parse program inputs
		Path outputPath;
		try {
			outputPath = new Path(args[0]);
			if(!"".equals(args[1])) {
				//Required for AWS
				String hbaseSite = args[1]; 
				config.addResource(new File(hbaseSite).toURI().toURL());
			}
		} catch (Exception e) {
			System.out.println("Error in parsing the program arguments");
			System.out.println("Usage: <outputPath> <hbaseSite>");
			throw e;
		}
				
		//Set up the HCompute job
		HBaseAdmin.available(config);
		Job job = Job.getInstance(config, "HCompute");
		job.setJarByClass(HCompute.class);
		job.setMapperClass(HComputeMapper.class);
		job.setPartitionerClass(HComPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(HComputeReducer.class);
		job.setMapOutputKeyClass(AirlineMonthPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		TableMapReduceUtil.initTableMapperJob(HTABLE_NAME, setScan(), HComputeMapper.class, AirlineMonthPair.class, IntWritable.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * Helper method to set the scan 
	 * @return Scan setting
	 */
	public static Scan setScan() {
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		scan.setFilter(setFilter());
		return scan;
	}
	
	/**
	 * Helper method to set the filter list
	 * @return the FilterList setting
	 */
	public static FilterList setFilter() {
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter yearFilter = new SingleColumnValueFilter(
				COLUMN_FAMILY, COLUMN_YEAR,
				CompareOperator.EQUAL, TARGET_YEAR);
		filterList.addFilter(yearFilter);
		SingleColumnValueFilter cancelledFilter = new SingleColumnValueFilter(
				COLUMN_FAMILY, COLUMN_CANCELLED,
				CompareOperator.EQUAL, NOT_CANCELLED);
		filterList.addFilter(cancelledFilter);
		return filterList;
	}
}
