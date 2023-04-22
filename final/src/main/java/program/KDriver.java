package program;

import calc.KMeans;
import calc.Silhouette;
import population.CGenerator;
import population.MinMax;
import population.PopulatePt;

public class KDriver {
	
	/**
	 * Main Driver method
	 * @param args 
	 * args[0] - input path for the data files
	 * args[1] - intermediate path for the intermediate files
	 * args[2] - string representation of the kvalue 
	 * args[3] - string representation of the max limit for KMeans iteration
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		String inputPath = args[0];
		String intermediatePath = args[1];
		String kvalue = args[2];
		String itrLimit = args[3];
		if(args[4].equals("yes")) {
			KConfig.IS_AWS = true;
		}
		String minMaxDir = intermediatePath + KConfig.MINMAX_DIR;
		String centroidsDir = intermediatePath + KConfig.CENTROID_FILE;
		PopulatePt.main(new String[] {inputPath, "20"});
		//MinMax.main(new String[] {intermediatePath + KConfig.MINMAX_DIR});
		//CGenerator.main(new String[] {minMaxDir + KConfig.RFILE_POSTFIX, centroidsDir, kvalue});
		//KMeans.main(new String[] {intermediatePath, intermediatePath, itrLimit});
		Silhouette.main(new String[] {"5", "1"});
	}
}
