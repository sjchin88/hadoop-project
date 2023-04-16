package program;

public class KConfig {
	public static boolean IS_AWS = false;
	public static final String HBASE_SITE= "/etc/hbase/conf/hbase-site.xml";
	public static final String HTABLE_NAME = "PickUpData";
	public static final byte[] COLUMN_FAMILY = "Coordinate".getBytes();
	public static final byte[] COLUMN_LATITUDE = "lat".getBytes(); 
	public static final byte[] COLUMN_LONGITUDE = "long".getBytes(); 
	public static final byte[] COLUMN_COUNT= "count".getBytes(); 
	public static final byte[] COLUMN_NEARESTC= "nearest".getBytes(); 
	public static final String MINMAX_DIR = "/minmax";
	public static final String CENTROID_FILE = "/centroids.txt";
	public static final String RFILE_POSTFIX = "/part-r-00000";
	
}
