package calc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Centroid implements WritableComparable<Centroid> {
	private IntWritable index = new IntWritable(0);
	private DoubleWritable latitude = new DoubleWritable(0);
	private DoubleWritable longitude = new DoubleWritable(0);
	
	
	public Centroid() {
		
	}
	
	public Centroid(int idx,double latitude, double longitude) {
		this.index.set(idx);
		this.latitude.set(latitude);
		this.longitude.set(longitude);
	}
	
	/**
	 * Getter for the idx
	 * @return IntWritable
	 */
	public IntWritable getIdx() {
		return this.index;
	}
	
	/**
	 * Setter for the idx
	 * @param index
	 */
	public void setIdx(int idx) {
		this.index.set(idx);
	}
	
	/**
	 * Setter for the idx
	 * @param index
	 */
	public void setIdx(IntWritable idx) {
		this.index.set(idx.get());
	}
	
	/**
	 * Getter for the latitude
	 * @return DoubleWritable
	 */
	public DoubleWritable getLatitude() {
		return this.latitude;
	}
	
	/**
	 * Setter for the latitude
	 * @param lat new latitude in double
	 */
	public void setLatitude(Double lat) {
		this.latitude.set(lat);
	}
	
	/**
	 * Getter for the longitude
	 * @return DoubleWritable
	 */
	public DoubleWritable getLongitude() {
		return this.longitude;
	}
	
	/**
	 * Setter for the longitude
	 * @param lat new longitude in double
	 */
	public void setLongitude(Double longitude) {
		this.longitude.set(longitude);
	}
	
	
	/**
	 * Write the data out
	 */
	public void write(DataOutput out) throws IOException {
		this.index.write(out);
		this.latitude.write(out);
		this.longitude.write(out);
	}

	/**
	 * Read the data in
	 */
	public void readFields(DataInput in) throws IOException {
		this.index.readFields(in);
		this.latitude.readFields(in);
		this.longitude.readFields(in);
	}

	/**
	 * Custom compareTo, 
	 */
	public int compareTo(Centroid other) {
		return this.index.compareTo(other.getIdx());
	}
	
	@Override
    public String toString() {
        String result = this.index.toString()+"," 
        		+ this.latitude.toString() + ","
        		+ this.longitude.toString();
        return result;
    }
}
