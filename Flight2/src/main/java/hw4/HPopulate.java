package hw4;

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
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

/**
 * Program to populate the HBase table with selected data from FlightData
 * Using BufferedMutator of size 20 mb,and rowKey of CarrierId-Month-DayOfMonth-Year-Flightnumber-Origin
 * @author csj
 *
 */
public class HPopulate {
	private static final String HTABLE_NAME = "FlightData";
	private static final byte[] COLUMN_FAMILY = "Flight".getBytes();
	private static final byte[] COLUMN_YEAR = "yr".getBytes(); 
	private static final byte[] COLUMN_CANCELLED = "cancel".getBytes(); 
	private static final byte[] COLUMN_DELAY = "delay".getBytes(); 
	private static int BUFFER_SIZEMB = 20;
	
	/**
	 * HPopulateMapper to read from the csv file and write into the HBaseTable
	 * @author csj
	 *
	 */
	public static class HPopulateMapper extends Mapper <LongWritable, Text, ImmutableBytesWritable, Writable> {
		private CSVParser csvParser;
		private Connection connection;
		private BufferedMutator mutator;
		
		/**
		 * Set up method before each Map Task
		 * Initialize the csvParser and get the BufferedMutator connection
		 */
		protected void setup(Context context) throws IOException  {
			this.csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
			
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
			params.writeBufferSize( BUFFER_SIZEMB * 1023 * 1024 );
			this.connection = ConnectionFactory.createConnection(config);
			this.mutator = this.connection.getBufferedMutator(params);	
			
		}
		
		/**
		 * Map method, construct the row key and add the key-value pairs into HBasetable
		 * key-value pairs include year, cancelled, delay
		 */
		public void map(LongWritable key, Text lineText, Context context) throws IOException {
			String line = lineText.toString();
			String[] records = this.csvParser.parseLine(line);
			StringBuilder rowKey = new StringBuilder();
			if (records.length > 0) {
				rowKey.append(records[6] + "-"); //Unique Carrier id
				rowKey.append(records[2] + "-"); //Month
				rowKey.append(records[3] + "-"); //Day of Month
				rowKey.append(records[0] + "-"); //Year
				rowKey.append(records[10] + "-"); //Flightnumber
				rowKey.append(records[11]); //Origin
				int year = Integer.valueOf(records[0]);
				double delayD = "".equals(records[37]) || records[37] == null ? 0F: Double.valueOf(records[37]);
				int delay = (int)delayD;
				boolean cancelled = Double.valueOf(records[41]) == 1.00 ? true : false;
				Put record = new Put(rowKey.toString().getBytes());
				record.addColumn(COLUMN_FAMILY, COLUMN_YEAR, Bytes.toBytes(year));
				record.addColumn(COLUMN_FAMILY, COLUMN_CANCELLED, Bytes.toBytes(cancelled));
				record.addColumn(COLUMN_FAMILY, COLUMN_DELAY, Bytes.toBytes(delay));
				this.mutator.mutate(record);
			}
		} // end of map
		
		/**
		 * Cleanup will be called once per Map task after all map function calls
		 * to close the HBase table connection
		 * @throws IOException 
		 */
		protected void cleanup(Context context) throws IOException {
			this.mutator.close();
			this.connection.close();
		}
	} // End of HPopulateMapper class
	
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
            admin.disableTable(tName);
            admin.deleteTable(tName);
            TableDescriptor desc = tdb.build();
            admin.createTable(desc);
            System.out.println(" Table recreated ");
        }
	} // end of createHTable() method
	
	/**
	 * Driver method
	 * @param args
	 * @throws Exception 
	 */
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
			BUFFER_SIZEMB = bufferSize;
		} catch (Exception e) {
			System.out.println("Error in parsing the buffer size, default will be used");
			BUFFER_SIZEMB = 20;
		}
		
		Connection connection = ConnectionFactory.createConnection(config);
		// Create new HBase table
		createHTable(connection);
				
		// Configure the HPopulate job
		Job job = Job.getInstance(config, "Populate HTable by Buffered v1");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(HPopulateMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);		
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, HTABLE_NAME);
		
		// set number of reducer task to 0 as not required
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, inputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
