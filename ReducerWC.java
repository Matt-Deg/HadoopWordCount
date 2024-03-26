package wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWC extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	//hadoop integer variable for word frequency
	private IntWritable frequency = new IntWritable();

	//overwritten reducer function
	//implictly reads in shuffled input
	//each iterable value in 'Iterable<IntWritable> values' is a shuffled grouping
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
      //frequency of current word initialized to 0
      int current_count = 0;
      
      //Count up the current word's frequency and write it to the output
      for (IntWritable val : values) {
        current_count += val.get();
      }
      
      frequency.set(current_count);
      
      context.write(key, frequency);
    }
    
}
