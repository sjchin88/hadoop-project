package population;

import java.io.File;
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
import program.KConfig;

/**
 * Program to populate the points from data files to HBase table
 * @author csj
 *
 */
public class PopulatePt {
	
	private static int BUFFER_SIZEKB = 20;
	
	/**
	 * Custom mapper class, 
	 * @author csj
	 *
	 */
	public static class PopulateMapper extends Mapper<LongWritable, Text, Coordinate, IntWritable>{
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
		private Coordinate pointKey = new Coordinate();
		private IntWritable countValue = new IntWritable(1);
		
		/**
		 * read pick-up locations from data files and emit point - count (key-value) pair to the reducer
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] records = this.csvParser.parseLine(line);
			double lat = Double.parseDouble(records[1]);
			double longi = Double.parseDouble(records[2]);
			pointKey.setLatitude(lat);
			pointKey.setLongitude(longi);
			context.write(pointKey,countValue);
		}
	} // End of Mapper class
	
	/**
	 * Custom reducer, 
	 * @author csj
	 *
	 */
	public static class PopulateReducer extends TableReducer<Coordinate, IntWritable, ImmutableBytesWritable> {
		private Connection connection;
		private BufferedMutator mutator;
		private int prefixItr = 0;
		/**
		 * Set up method before each Reduce Task
		 * Initialize the BufferedMutator connection
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
			BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(KConfig.HTABLE_NAME)).listener(listener);
			//BufferSize in byte, default is 20
			params.writeBufferSize( BUFFER_SIZEKB * 1024 );
			this.connection = ConnectionFactory.createConnection(config);
			this.mutator = this.connection.getBufferedMutator(params);		
		} //end of set up
		
		/**
		 * sum the count for the same coordinate then write the coordinate and count information to the HBase
		 */
	    public void reduce(Coordinate key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
	    	int sum = 0;
		    for (IntWritable val : values) {
		        sum += val.get();
		    }
		    String prefix = ""+(prefixItr % 100);
		    prefixItr++;
		    String rowKey = prefix+key.getLatitude().toString()+ key.getLongitude().toString();
	      
	        Put record = new Put(rowKey.toString().getBytes());
	        record.addColumn(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LATITUDE, Bytes.toBytes(key.getLatitude().get()));
			record.addColumn(KConfig.COLUMN_FAMILY, KConfig.COLUMN_LONGITUDE, Bytes.toBytes(key.getLongitude().get()));
			record.addColumn(KConfig.COLUMN_FAMILY, KConfig.COLUMN_COUNT, Bytes.toBytes(sum));
			record.addColumn(KConfig.COLUMN_FAMILY, KConfig.COLUMN_NEARESTC, Bytes.toBytes(0));
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
        TableName tName = TableName.valueOf(KConfig.HTABLE_NAME);
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tName);
        //tdb.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
        //tdb.setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy");
       // tdb.setValue("KeyPrefixRegionSplitPolicy.prefix_length", "2");
       // tdb.setValue("hbase.increasing.policy.initial.size", "1024");
        //tdb.setMaxFileSize(1024);
        //tdb.setMemStoreFlushSize(1024);
        //tdb.setValue("hbase.table.sanity.checks", "false");
        //
        
        ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(KConfig.COLUMN_FAMILY);
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
	
	public static void main(String[] args) throws Exception {
		// Instantiating configuration class
        Configuration config = HBaseConfiguration.create(new Configuration());
        
		// Parse program inputs
		Path inputPath;
		int bufferSize;
		try {
			inputPath = new Path(args[0]);
			if(KConfig.IS_AWS) { 
			//Required for AWS 
				config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL()); 
			}
			
		} catch (Exception e) {
			System.out.println("Error in parsing the program arguments");
			System.out.println("Usage: <inputPath> <buffersize in mb (optional)>");
			throw e;
		}
		
		try {
			bufferSize = Integer.valueOf(args[1]);
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
				KConfig.HTABLE_NAME, 
				PopulateReducer.class,
				job);
		

		FileInputFormat.addInputPath(job, inputPath);
		job.waitForCompletion(true);
		System.out.println("Population Completed");
	}
}
