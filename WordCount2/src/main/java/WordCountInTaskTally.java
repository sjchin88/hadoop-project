import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * WordCount with In Task Tally
 * @author csj
 *
 */
public class WordCountInTaskTally {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

    private Map<String, Integer> countMap;
    
    // Initialize the countMap
    public void setup(Context context) {
    	countMap = new HashMap<String, Integer>();
    }

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        //Check if first character matched
        //And put it into the object's countMap if it does
        if(token.matches("^[mnopqMNOPQ].*")){
          countMap.put(token, countMap.getOrDefault(token, 0) + 1);
        }
      }
    }
    
    // Loop through the object's countMap and emit the record
    public void cleanup(Context context) throws IOException, InterruptedException {
    	Text word = new Text();
    	IntWritable countInt = new IntWritable();
    	for (String token:countMap.keySet()) {
      	  word.set(token);
      	  countInt.set(countMap.get(token));
      	  context.write(word, countInt);
        }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  //Custom partitioner
  public static class WordPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// Custom partition rule
		char first = key.toString().charAt(0);
		first = Character.toLowerCase(first);
		int target = first - 'm';
		// if first is m / M , target will be 0
		// if first is n / N , target will be 1 and so on
		return target % numPartitions;
	}
	  
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCountInTaskTally.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setPartitionerClass(WordPartitioner.class);
    // Disable the combiner
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
