package hw4;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;


/**
 * Program to populate the HBase table with selected data from FlightData
 * using bulk loading technique
 * @author csj
 *
 */
public class HPopulateV1 {
	public static final String HTABLE_NAME = "FlightData";
	private static final byte[] COLUMN_FAMILY = "Flight".getBytes();
	private static final byte[] COLUMN_YEAR = "yr".getBytes(); 
	private static final byte[] COLUMN_CANCELLED = "cancel".getBytes(); 
	private static final byte[] COLUMN_DELAY = "delay".getBytes(); 
	
	/**
	 * HPopulateMapper to read from the csv file and write into the HBaseTable
	 * Using BulkLoading
	 * @author csj
	 *
	 */
	public static class HPopulateMapper extends Mapper <LongWritable, Text, ImmutableBytesWritable, Put> {
		private CSVParser csvParser;
		private ImmutableBytesWritable ibw;
		
		/**
		 * Set up method before each Map Task
		 * Initialize the csvParser and get the HBasetable connection
		 */
		protected void setup(Context context) throws IOException  {
			this.csvParser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(false).build();
			this.ibw = new ImmutableBytesWritable();
		}
		
		/**
		 * Map method, construct the row key and add the key-value pairs into HBasetable
		 * key-value pairs include year, cancelled, delay
		 * @throws InterruptedException 
		 */
		public void map(LongWritable key, Text lineText, Context context) throws IOException, InterruptedException {
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
				this.ibw.set(rowKey.toString().getBytes());
				context.write(ibw, record);
			}
		} // end of map
		
	} // End of HPopulateMapper class
	
	
	/**
	 * Driver method
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// Configure the HPopulate job
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create(new Configuration());
		        
		// Parse program inputs
		Path inputPath;
		Path outputPath;
		String tempPath;
		try {
			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);
			if(!"".equals(args[2])) {
				 //Required for AWS
		        String hbaseSite = args[2]; 
				config.addResource(new File(hbaseSite).toURI().toURL());
			}
			if(args.length > 3) {
				tempPath = args[3];
				config.set("hbase.fs.tmp.dir", tempPath);
			}
		} catch (Exception e) {
			System.out.println("Error in parsing the program arguments");
			System.out.println("Usage: <inputPath> <outputPath> <hbaseSite> <tempPath>");
			throw e;
		}
				
		// Set up the job
		Job job = Job.getInstance(config, "Populate HTable by Bulk Loading");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(HPopulateMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// Create the connection
        Connection connection = ConnectionFactory.createConnection(config);
       
        // Instantiating tableName
        TableName tName = TableName.valueOf(HTABLE_NAME);
        createHTable(connection, tName);
        
        // Set up HFileOutputFormat2
        HFileOutputFormat2.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(HFileOutputFormat2.class);
		
		Table table = connection.getTable(tName);
		HFileOutputFormat2.configureIncrementalLoad(job, table, table.getRegionLocator());
		System.out.println("Map reduce job start");
		
		job.waitForCompletion(true);
		if(job.isSuccessful()) {
			BulkLoadHFiles loadFfiles = new BulkLoadHFilesTool(config);
			loadFfiles.bulkLoad(tName, new Path(args[1]));
			System.exit(0);
		}else {
			System.exit(1);
		}
	}
	
	/**
	 * Helper method to initialize the HTable
	 * @throws IOException
	 */
	public static void createHTable(Connection connection, TableName tName) throws IOException {
        
        // Instantiating HbaseAdmin class
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
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
}
