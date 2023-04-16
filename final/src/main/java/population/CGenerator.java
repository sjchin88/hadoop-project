package population;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Text;

import calc.Centroid;

public class CGenerator {
	private static int KVALUE = 5;
	private static double minLatitude;
	private static double maxLatitude;
	private static double minLongitude;
	private static double maxLongitude;
	
	public static void readMinMax(String inputDir) throws NumberFormatException, IOException {
		String line;
		BufferedReader bufferedReader = new BufferedReader(
				new FileReader(inputDir));
		try {
			// Read the file split by the splitter and store it in
			// the list
			while ((line = bufferedReader.readLine()) != null) {
				String[] temp = line.split("\\s+");
				String[] values = temp[1].split(",");
				double minV = Double.parseDouble(values[0]);
				double maxV = Double.parseDouble(values[1]);
				if(temp[0].equals("Latitude")) {
					minLatitude = minV;
					maxLatitude = maxV;
				}else {
					minLongitude = minV;
					maxLongitude = maxV;
				}
			}
		} finally {
			bufferedReader.close();
		}
	}
	
	public static void generateC(String outputDir) throws IOException {
		Centroid center = new Centroid();
		Random random = new Random();
		//System.out.println(minLat + ":"+maxLat);
		//System.out.println(minLongi + ":"+maxLongi);
		BufferedWriter bwriter = new BufferedWriter(new FileWriter(outputDir));
		for(int i = 0; i < KVALUE; i++) {
			center.setIdx(i);
			double lat = random.nextDouble() * (maxLatitude - minLatitude) + minLatitude;
			double longi = random.nextDouble() * (maxLongitude - minLongitude) + minLongitude;
			center.setLatitude(lat);
			center.setLongitude(longi);
			//System.out.println(center.toString());
			bwriter.write(center.toString());
			bwriter.newLine();
			//context.write(new Text(center.toString()), null);
		}
		bwriter.close();
	}
	
	public static void main(String[] args) {
		String inputDir = args[0];
		String outputDir = args[1];
		KVALUE = Integer.parseInt(args[2]);
		try {
			readMinMax(inputDir);
			generateC(outputDir);
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("Error in generating centroids");
		}
		System.out.println("Centroids generated");
	}
}
