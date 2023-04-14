package population;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import calc.Centroid;
import calc.Coordinate;

public class PopulatePt {
	private static final String HTABLE_NAME = "PickUpData";
	private static final byte[] COLUMN_FAMILY = "Coordinate".getBytes();
	private static final byte[] COLUMN_LATITUDE = "lat".getBytes(); 
	private static final byte[] COLUMN_LONGITUDE = "long".getBytes(); 
	private static final byte[] COLUMN_COUNT= "count".getBytes(); 
	private static final byte[] COLUMN_NEARESTC= "nearest".getBytes(); 
	private static int BUFFER_SIZEKB = 20;
	
	public static class PopulateMapper extends Mapper<LongWritable, Text, Coordinate, IntWritable>{
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
		private Coordinate pointKey = new Coordinate();
		private IntWritable countValue = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] records = this.csvParser.parseLine(line);
			double lat = Double.parseDouble(records[1]);
			double longi = Double.parseDouble(records[2]);
			pointKey.setLatitude(lat);
			pointKey.setLongitude(longi);
			context.write(pointKey,countValue);
		}
	} // End of Mapper class
	
	public static class PopulateReducer extends TableReducer<Coordinate, IntWritable, ImmutableBytesWritable> {
		private Connection connection;
		private BufferedMutator mutator;
		
		/**
		 * Set up method before each Map Task
		 * Initialize the csvParser and get the BufferedMutator connection
		 */
		protected void setup(Context context) throws IOException  {
			
			// Instantiating the connection to HBase table via BufferedMutator
			Configuration config = HBaseConfiguration.create();
			BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
				
				public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator)
						throws RetriesExhaustedWithDetailsException {
					for (int i = 0; i < exception.getNumExceptions(); i++) {
				        System.out.println("Failed to sent put " + exception.getRow(i) + ".");              
				    }					
				}
			};
			BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(HTABLE_NAME)).listener(listener);
			//BufferSize in byte, default is 20
			params.writeBufferSize( BUFFER_SIZEKB * 1024 );
			this.connection = ConnectionFactory.createConnection(config);
			this.mutator = this.connection.getBufferedMutator(params);	
			
		}
		
	    public void reduce(Coordinate key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
	    	int sum = 0;
		    for (IntWritable val : values) {
		        sum += val.get();
		    }
		    String rowKey = key.getLatitude().toString()+ key.getLongitude().toString();
		    System.out.println(rowKey);
	      
	        Put record = new Put(rowKey.toString().getBytes());
	        record.addColumn(COLUMN_FAMILY, COLUMN_LATITUDE, Bytes.toBytes(key.getLatitude().get()));
			record.addColumn(COLUMN_FAMILY, COLUMN_LONGITUDE, Bytes.toBytes(key.getLongitude().get()));
			record.addColumn(COLUMN_FAMILY, COLUMN_COUNT, Bytes.toBytes(sum));
			record.addColumn(COLUMN_FAMILY, COLUMN_NEARESTC, Bytes.toBytes(0));
			this.mutator.mutate(record);
	    }
	    
	    /**
		 * Cleanup will be called once per Map task after all map function calls
		 * to close the HBase table connection
		 * @throws IOException 
		 */
		protected void cleanup(Context context) throws IOException {
			this.mutator.close();
			this.connection.close();
		}
	  } // End of reducer
	
	/**
	 * Helper method to initialize the HTable
	 * @throws IOException
	 */
	public static void createHTable(Connection connection) throws IOException {    
        // Instantiating HbaseAdmin class
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
        TableName tName = TableName.valueOf(HTABLE_NAME);
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tName);
        
        ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY);
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
	
	public static void main(String[] args) throws Exception {
		// Instantiating configuration class
        Configuration config = HBaseConfiguration.create(new Configuration());
        
		// Parse program inputs
		Path inputPath;
		int bufferSize;
		try {
			inputPath = new Path(args[0]);
			/*
			 * if(!"".equals(args[1])) { //Required for AWS String hbaseSite = args[1];
			 * config.addResource(new File(hbaseSite).toURI().toURL()); }
			 */
		} catch (Exception e) {
			System.out.println("Error in parsing the program arguments");
			System.out.println("Usage: <inputPath> <hbaseSite> <buffersize in mb (optional)>");
			throw e;
		}
		
		try {
			bufferSize = Integer.valueOf(args[2]);
			BUFFER_SIZEKB = bufferSize;
		} catch (Exception e) {
			System.out.println("Error in parsing the buffer size, default will be used");
			BUFFER_SIZEKB = 20;
		}
		
		Connection connection = ConnectionFactory.createConnection(config);
		// Create new HBase table
		createHTable(connection);
				
		// Configure the HPopulate job
		Job job = Job.getInstance(config, "Populate HTable by Buffered v1");
		job.setJarByClass(PopulatePt.class);
		job.setMapperClass(PopulateMapper.class);
		job.setMapOutputKeyClass(Coordinate.class);
		job.setMapOutputValueClass(IntWritable.class);		
		job.setOutputFormatClass(TableOutputFormat.class);
		TableMapReduceUtil.initTableReducerJob(
				HTABLE_NAME, 
				PopulateReducer.class,
				job);
		
		// set number of reducer task to 0 as not required
		FileInputFormat.addInputPath(job, inputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
