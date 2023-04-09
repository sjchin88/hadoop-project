package hw4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Custom pair for Airline and Month composite key
 * @author csj
 *
 */
public class AirlineMonthPair implements WritableComparable<AirlineMonthPair> {
	private Text airline = new Text("");
	private IntWritable month = new IntWritable(0);
	
	/**
	 * Create AirlineMonthPair with empty value
	 */
	public AirlineMonthPair() {
		this.setAirline("");
		this.setMonth(0);
	}
	
	/**
	 * Create AirlineMonthPair with the arguments
	 * @param airline   airline id
	 * @param second	month in string
	 */
	public AirlineMonthPair(String airline, String month) {
		this.setAirline(airline);
		this.setMonth(month);
	}

	/**
	 * Getter for the airline
	 * @return airline in Text
	 */
	public Text getAirline() {
		return this.airline;
	}
	
	/**
	 * Setter for the airline
	 * @param airline
	 */
	public void setAirline(String airline) {
		this.airline.set(airline);
	}
	
	/**
	 * Getter for the month
	 * @return month
	 */
	public IntWritable getMonth() {
		return this.month;
	}
	
	/**
	 * Setter for the month
	 * @param month
	 */
	public void setMonth(String month) {
		this.setMonth(Integer.valueOf(month));
	}
	
	/**
	 * Setter for the month
	 * @param month in int
	 */
	public void setMonth(int month) {
		this.month.set(month);
	}
	
	/**
	 * Write the data out
	 */
	public void write(DataOutput out) throws IOException {
		this.airline.write(out);
		this.month.write(out);
	}

	/**
	 * Read the data in
	 */
	public void readFields(DataInput in) throws IOException {
		this.airline.readFields(in);
		this.month.readFields(in);
	}

	/**
	 * Custom compareTo, primaryly sort by name, 
	 * secondary sort by month
	 */
	public int compareTo(AirlineMonthPair other) {
		int cmp = this.getAirline().compareTo(other.getAirline());
		if(cmp != 0) {
			return cmp;
		}
		int month1 = this.getMonth().get();
		int month2 = other.getMonth().get();
		if (month1 == month2) {
			return 0;
		} else if (month1 < month2) {
			return -1;
		} else {
			return 1;
		}
	}
}
