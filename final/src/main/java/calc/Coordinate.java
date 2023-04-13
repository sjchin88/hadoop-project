package calc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Coordinate implements WritableComparable<Coordinate> {
	private DoubleWritable latitude = new DoubleWritable(0);
	private DoubleWritable longitude = new DoubleWritable(0);
	private IntWritable count = new IntWritable(0);
	
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
	 * Getter for the count
	 * @return IntWritable
	 */
	public IntWritable getCount() {
		return this.count;
	}
	
	/**
	 * Setter for the count
	 * @param count
	 */
	public void setCount(int count) {
		this.count.set(count);
	}
	
	/**
	 * Increment count by one
	 */
	public void incrCount() {
		int curr_count = this.count.get();
		curr_count++;
		this.count.set(curr_count);
	}
	/**
	 * Write the data out
	 */
	public void write(DataOutput out) throws IOException {
		this.latitude.write(out);
		this.longitude.write(out);
		this.count.write(out);
	}

	/**
	 * Read the data in
	 */
	public void readFields(DataInput in) throws IOException {
		this.latitude.readFields(in);
		this.longitude.readFields(in);
		this.count.readFields(in);
	}

	/**
	 * Custom compareTo, 
	 */
	public int compareTo(Coordinate other) {
		int cmp = this.getLatitude().compareTo(other.getLatitude());
		if(cmp != 0) {
			return cmp;
		}
		double long1 = this.getLongitude().get();
		double long2 = other.getLongitude().get();
		if (long1 == long2) {
			return 0;
		} else if (long1 < long2) {
			return -1;
		} else {
			return 1;
		}
	}
	
	@Override
    public String toString() {
        String result = this.latitude.toString() + ","
        		+ this.longitude.toString() + ","
        		+ this.getCount().toString();
        return result;
    }
}
