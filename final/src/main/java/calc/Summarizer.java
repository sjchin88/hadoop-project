package calc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import program.KConfig;

/**
 * Find max k value and emit the corresponding data to file
 * @author csj
 *
 */
public class Summarizer {
	private static double maxSil = -1.0;
	private static int kMaxSil;
	private static int itrMaxSil = 0;
	
	/**
	 * Driver method
	 * @param args args[0] = output file name
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		String outputDir = args[0];
		//String kFName = args[1];
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
								        
		if(KConfig.IS_AWS) {
			//Required for AWS
			config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL());
		}
		HBaseAdmin.available(config);
		
		//Loop through all k value silscore calculation to get the max SilHouette Score
		readK(config, outputDir);
		
		//Output the SilHouette Score, KValue, and associated Centroids to the file 
		printK(config, outputDir);
	}
	
	public static void readK(Configuration config, String outputDir) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		Table sTable = connection.getTable(TableName.valueOf(KConfig.TABLE_SILSCORE));
		FileSystem fileSystem = FileSystem.get(URI.create(outputDir),config);
		System.out.println(fileSystem.getUri());
		FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(outputDir+"//KResults.txt"));      
		PrintWriter writer  = new PrintWriter(fsDataOutputStream);
		//BufferedWriter bwriter = new BufferedWriter(fsDataOutputStream);
		Scan scan = new Scan();
		ResultScanner rs = sTable.getScanner(scan);
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
		    // process result...
				double silscore = Bytes.toDouble(r.getValue(KConfig.CF_SILSCORE, KConfig.COLUMN_SILSCORE));
				int k = Bytes.toInt(r.getValue(KConfig.CF_SILSCORE, KConfig.COLUMN_K));
				int itr = Bytes.toInt(r.getValue(KConfig.CF_SILSCORE, KConfig.COLUMN_ITERATION));
				if(silscore > maxSil) {
					maxSil = silscore;
					kMaxSil = k;
					itrMaxSil = itr;
				}
				writer.println("SilScore:" + silscore + " at Kvalue:" + k + " at iteration:"+itr);
			}
		} finally {
			rs.close();  // always close the ResultScanner!
			writer.close();
		}
	}
	
	public static void printK(Configuration config, String outputDir) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		FileSystem fileSystem = FileSystem.get(URI.create(outputDir),config);
		System.out.println(fileSystem.getUri());
		FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(outputDir+"//Summary.txt"));      
		PrintWriter bwriter  = new PrintWriter(fsDataOutputStream);
		bwriter.println("Max SilScore:" + maxSil + " at kValue:" + kMaxSil + " at iteration:" + itrMaxSil);
		//bwriter.newLine();
		bwriter.println("Corresponding centroids");
		//bwriter.newLine();
		Table cTable = connection.getTable(TableName.valueOf(KConfig.TABLE_CENTROID));
		
		Scan scan = KMeans.setCentroidScan(itrMaxSil, kMaxSil);
		ResultScanner rs = cTable.getScanner(scan);
		try {
			for (Result r = rs.next(); r != null; r = rs.next()) {
		    // process result...
				int idx = Bytes.toInt(r.getValue(KConfig.CF_CENTROID, KConfig.COLUMN_IDX));
				double lat = Bytes.toDouble(r.getValue(KConfig.CF_CENTROID, KConfig.COLUMN_LATITUDE));
				double longi = Bytes.toDouble(r.getValue(KConfig.CF_CENTROID, KConfig.COLUMN_LONGITUDE));
				bwriter.println(idx+". Lat:"+lat + " Longitude:"+longi);
				//bwriter.newLine();
			}
		} finally {
			rs.close();  // always close the ResultScanner!
		}
		bwriter.close();
	}
	
	
}
