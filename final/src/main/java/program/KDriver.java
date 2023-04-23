package program;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;


import calc.KMeans;
import calc.Summarizer;
import population.CGenerator;
import population.MinMax;
import population.PopulatePt;

/**
 * Represent the main driver class
 * @author csj
 *
 */
public class KDriver {
	
	/**
	 * Main Driver method
	 * @param args 
	 * args[0] - input path for the data files
	 * args[1] - intermediate path for the intermediate files
	 * args[2] - string representation of the kvalue limit
	 * args[3] - string representation of the kvalue step rate
	 * args[4] - string representation of the max limit for KMeans iteration
	 * args[5] - 'yes' if execution with AWS environment
	 * args[6] - job numbers
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		String inputPath = args[0];
		String outPath = args[1];
		String kLimit = args[2];
		int kLimitInt = Integer.valueOf(kLimit);
		int kStep = Integer.valueOf(args[3]);
		String itrLimit = args[4];
		if(args[5].equals("yes")) {
			KConfig.IS_AWS = true;
		}
		String jobNums = args[6];
		
		
		
		// Create new HBase table
		resetHTable();
		
		//Read pick-up locations from data into HBase and get the minmax
		PopulatePt.main(new String[] {inputPath, jobNums});
		MinMax.main(new String[] {});
		for(int k = kStep; k < kLimitInt; k += kStep) {
			CGenerator.main(new String[] {""+k});
			KMeans.main(new String[] {itrLimit, ""+k, jobNums});
		}
		Summarizer.main(new String[] {outPath + "summary.txt", outPath + "ksummary.txt"});
	}
	
	/**
	 * Helper method to reset the HTable
	 * @throws IOException
	 */
	public static void resetHTable() throws IOException {   
		// Instantiating configuration class
		Configuration config = HBaseConfiguration.create();
										        
		if(KConfig.IS_AWS) {
		//Required for AWS
			config.addResource(new File(KConfig.HBASE_SITE).toURI().toURL());
		}
		HBaseAdmin.available(config);
		Connection connection = ConnectionFactory.createConnection(config);
        // Instantiating HbaseAdmin class
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
        TableName tName = TableName.valueOf(KConfig.TABLE_PT);
        // delete hTable if exist
        if (admin.tableExists(tName)) {
        	System.out.println(tName.getNameAsString() + " Table already exists ");
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
        
        tName = TableName.valueOf(KConfig.TABLE_MINMAX);
        // delete hTable if exist
        if (admin.tableExists(tName)) {
        	System.out.println(tName.getNameAsString() + " Table already exists ");
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
        
        tName = TableName.valueOf(KConfig.TABLE_CENTROID);
        // delete hTable if exist
        if (admin.tableExists(tName)) {
        	System.out.println(tName.getNameAsString() + " Table already exists ");
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
        
        tName = TableName.valueOf(KConfig.TABLE_SILSCORE);
        // delete hTable if exist
        if (admin.tableExists(tName)) {
        	System.out.println(tName.getNameAsString() + " Table already exists ");
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
	} // end of createHTable() method
}
