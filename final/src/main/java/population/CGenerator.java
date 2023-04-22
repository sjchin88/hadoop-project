package population;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import calc.Centroid;
import program.KConfig;

public class CGenerator {
	private static int KVALUE = 5;
	private static double minLatitude;
	private static double maxLatitude;
	private static double minLongitude;
	private static double maxLongitude;
	
	/**
	 * Helper method to initialize the HTable
	 * @throws IOException
	 */
	public static void createHTable(Connection connection) throws IOException {    
        // Instantiating HbaseAdmin class
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
        TableName tName = TableName.valueOf(KConfig.TABLE_CENTROID);
        TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tName);
        
        ColumnFamilyDescriptorBuilder cfd = ColumnFamilyDescriptorBuilder.newBuilder(KConfig.CF_CENTROID);
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
	
	public static void readMinMax(Connection connection) throws NumberFormatException, IOException, InterruptedException {
		Table hTable = connection.getTable(TableName.valueOf(KConfig.TABLE_MINMAX));
		
		Scan scan = new Scan();
		ResultScanner rs = hTable.getScanner(scan);
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
		    // process result...
				double min = Bytes.toDouble(r.getValue(KConfig.CF_MINMAX, KConfig.COLUMN_MIN));
				double max = Bytes.toDouble(r.getValue(KConfig.CF_MINMAX, KConfig.COLUMN_MAX));
				if(Bytes.toString(r.getRow()).equals("Latitude")) {
					minLatitude = min;
					maxLatitude = max;
				}else {
					minLongitude = min;
					maxLongitude = max;
				}
			}
		} finally {
			rs.close();  // always close the ResultScanner!
			System.out.println("Latitude:" + minLatitude + "-"+maxLatitude);
			System.out.println("Longitude:" + minLongitude + "-"+maxLongitude);
		}
	}
	
	public static void generateC(Connection connection) throws IOException {
		//Centroid center = new Centroid();
		Random random = new Random();
		Table cTable = connection.getTable(TableName.valueOf(KConfig.TABLE_CENTROID));
		for(int i = 0; i < KVALUE; i++) {
			String rowKey = "Initial"+i;
			double lat = random.nextDouble() * (maxLatitude - minLatitude) + minLatitude;
			double longi = random.nextDouble() * (maxLongitude - minLongitude) + minLongitude;
			Put record = new Put(rowKey.getBytes());
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_LATITUDE, Bytes.toBytes(lat));
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_LONGITUDE, Bytes.toBytes(longi));
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_IDX, Bytes.toBytes(i));
	        record.addColumn(KConfig.CF_CENTROID, KConfig.COLUMN_ITERATION, Bytes.toBytes(0));
			//System.out.println(center.toString());
			cTable.put(record);
			//context.write(new Text(center.toString()), null);
		}
		cTable.close();
	}
	
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException, NumberFormatException, InterruptedException {
		String inputDir = args[0];
		String outputDir = args[1];
		KVALUE = Integer.parseInt(args[2]);
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
		readMinMax(connection);
		generateC(connection);
		System.out.println("Centroids generated");
	}
}
