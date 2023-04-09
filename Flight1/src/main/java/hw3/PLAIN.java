package hw3;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

/**
 * Plain mapreduce program for HW3
 * 
 * @author csj
 *
 */
public class PLAIN {
	private static final String ORI_AIRPORT = "ORD";
	private static final String DEST_AIRPORT = "JFK";
	private static final LocalDate START_DATE = LocalDate.of(2007, 6, 1);
	private static final LocalDate END_DATE = LocalDate.of(2008, 5, 31);
	private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("uuuu-MM-d");

	/**
	 * Custom global counter to calculate the average after all reduce task
	 * 
	 * @author csj
	 *
	 */
	public enum FlightCounter {
		TotalFlights, TotalDelay
	}

	/**
	 * First Mapper Class, perform the filtering
	 * 
	 * @author csj
	 *
	 */
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();

		/**
		 * Map function will filtered out columns not required, and rows not applicable
		 * based on origin, dest, cancelled and diverted information and write the
		 * required record with the connecting airport as key
		 */
		public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			String[] records = this.csvParser.parseLine(line);
			if (this.isValidFlight(records)) {
				// Set outKey = connecting airport:FlightDate
				Text outKey = new Text();
				if (ORI_AIRPORT.equals(records[11])) {
					outKey.set(records[17] + ":" + records[5]);
				} else {
					outKey.set(records[11] + ":" + records[5]);
				}
				Text outValue = new Text(this.extractValues(records));
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

			// 1. Check if neither origin airport nor destination airport not match
			if (!ORI_AIRPORT.equals(records[11]) && !DEST_AIRPORT.equals(records[17])) {
				return false;
			}

			// 1.a if both origin and destination airport match, this is single leg flight
			if (ORI_AIRPORT.equals(records[11]) && DEST_AIRPORT.equals(records[17])) {
				return false;
			}

			// 2 if flight is cancelled or diverted, return false
			if (records[41].equals("1.00") || records[43].equals("1.00")) {
				return false;
			}

			// 3. Check Flight Date
			LocalDate flightDate = LocalDate.parse(records[5], dtf);
			if (flightDate.compareTo(START_DATE) < 0 || flightDate.compareTo(END_DATE) > 0) {
				return false;
			}
			return true;
		}

		/**
		 * Extract required columns from the records
		 * 
		 * @param records String[] array of all the columns
		 * @return all values as a single String
		 */
		public String extractValues(String[] records) {
			StringBuilder sb = new StringBuilder();
			sb.append(records[11] + ",");// Origin
			sb.append(records[17] + ",");// Destination
			sb.append(records[24] + ",");// DepTime
			sb.append(records[35] + ",");// ArrTime
			sb.append(records[37]); // ArrDelayMinutes
			return sb.toString();
		}
	}

	/**
	 * Join Reducer class
	 * Count the sum and total delay, 
	 * add them to the hadoop counter
	 * @author CSJ
	 *
	 */
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		private List<String[]> origin_list = new ArrayList<String[]>();
		private List<String[]> dest_list = new ArrayList<String[]>();
		private int countFlights;
		private double totalDelay;

		/**
		 * Set up before every reduce Task before any map function call
		 */
		protected void setup(Context context) {
			this.countFlights = 0;
			this.totalDelay = 0.0;
		}

		/**
		 * Write the data into origin_list or dest_list based on value indicator Join
		 * the list afterward
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			origin_list.clear();
			dest_list.clear();
			for (Text value : values) {
				String[] record = value.toString().split(",");
				if (ORI_AIRPORT.equals(record[0])) {
					origin_list.add(record);
				} else {
					dest_list.add(record);
				}
			}
			this.join(key, context);
		}

		/**
		 * Join the information, if the flight dates of both flights matched
		 * 
		 * @param key
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void join(Text key, Context context) throws IOException, InterruptedException {
			int count = 0;
			double sum = 0;
			for (String[] oriRecord : origin_list) {
				for (String[] destRecord : dest_list) {
					// int firstDepTime = Integer.valueOf(oriRecord[3]);
					int firstArrTime = Integer.valueOf(oriRecord[3]);
					int secDepTime = Integer.valueOf(destRecord[2]);
					if (firstArrTime < secDepTime) {
						double delayFirst = Double.valueOf(oriRecord[4]);
						double delaySec = Double.valueOf(destRecord[4]);
						double delaySum = delayFirst + delaySec;
						count++;
						sum += delaySum;
					}
				}
			}
			this.countFlights += count;
			this.totalDelay += sum;
		}

		/**
		 * cleanup will be called after all reduce tasks completed
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.getCounter(FlightCounter.TotalFlights).increment(countFlights);
			context.getCounter(FlightCounter.TotalDelay).increment((long) totalDelay);
			Text k1 = new Text("Final:");
			long count = context.getCounter(FlightCounter.TotalFlights).getValue();
			long delay = context.getCounter(FlightCounter.TotalDelay).getValue();
			double average = delay * 1.0 / count;
			String result = "count:" + count + " average:" + average;
			System.out.println(result);
			context.write(k1, new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "flights join");
		job.setJarByClass(PLAIN.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
