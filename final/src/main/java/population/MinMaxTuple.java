package population;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;



public class MinMaxTuple implements WritableComparable<MinMaxTuple>{
	private DoubleWritable min = new DoubleWritable(0);
	private DoubleWritable max = new DoubleWritable(0);

	public DoubleWritable getMin() {
		return this.min;
	}
	
	public void setMin(double min) {
		this.min.set(min);
	}
	
	public DoubleWritable getMax() {
		return this.max;
	}
	
	public void setMax(double max) {
		this.max.set(max);
	}
	
		
	public void write(DataOutput out) throws IOException {
	
		this.min.write(out);
		this.max.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.min.readFields(in);
		this.max.readFields(in);
	}

	public int compareTo(MinMaxTuple o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
