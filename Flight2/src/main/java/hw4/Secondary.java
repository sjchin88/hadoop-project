package hw4;

import java.io.IOException;
import java.time.LocalDate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

/**
 * Compute the average delay of each airline for each month in TARGET_YEAR (2008) 
 * result sorted by airline (primary) and month (secondary)
 * @author csj
 *
 */
public class Secondary {
	private static final Integer TARGET_YEAR = 2008;
	
	/**
	 * Flight mapper Class, perform the filtering
	 * 
	 * @author csj
	 *
	 */
	public static class FlightMapper extends Mapper<LongWritable, Text, AirlineMonthPair, IntWritable> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
		private AirlineMonthPair outKey = new AirlineMonthPair();
		private IntWritable outValue = new IntWritable();
	
		/**
		 * Map function will filtered out columns not required, and rows not applicable
		 * write the required record with the connecting airport as key
		 */
		public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			String[] records = this.csvParser.parseLine(line);
			if (this.isValidFlight(records)) {
				// Set outKey = Airline + Month
				this.outKey.setAirline(records[6]);
				this.outKey.setMonth(records[2]);
				double delay = "".equals(records[37]) || records[37] == null ? 0F: Double.valueOf(records[37]);
				this.outValue.set((int)delay);
				context.write(outKey, outValue);
			}
		}
		
		/**
		 * Check if the flight is valid
		 * 
		 * @param records all data for particular flight
		 * @return boolean value
		 */
		public boolean isValidFlight(String[] records) {
			
			if (records==null || records.length == 0) {
				return false;
			}
			
			if("".equals(records[0]) || "".equals(records[2]) || "".equals(records[6])) {
				return false;
			}
			
			int year = Integer.valueOf(records[0]);
			// 1. Check if the year match
			if (year!=TARGET_YEAR) {
				return false;
			}

			// 2 if flight is cancelled  return false
			if (("1.00").equals(records[41])) {
				return false;
			}

			return true;
		}
	} // end of FlightMapper class
	
	/**
	 * Custom Partitioner class to partition based on the airline only from the composite key
	 * @author csj
	 *
	 */
	public static class FlightPartitioner extends Partitioner<AirlineMonthPair, IntWritable>{
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
	public static class FlightReducer extends Reducer<AirlineMonthPair, IntWritable, Text, Text> {
		
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
	} // End of FlightReducer
	
	/**
	 * Driver method
	 * @param args input output passed through from CLI
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Secondary sort program");
		job.setJarByClass(Secondary.class);
		job.setMapperClass(FlightMapper.class);
		job.setPartitionerClass(FlightPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(FlightReducer.class);
		job.setMapOutputKeyClass(AirlineMonthPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
