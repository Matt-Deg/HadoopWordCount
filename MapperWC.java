package wordCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperWC extends Mapper<Object, Text, Text, IntWritable>{

	//hadoop integer variable - initialized to 1
	 private IntWritable one = new IntWritable(1);
	 
	 //hadoop string variable - initialized empty
	 private Text word = new Text();

	 //overwritten hadoop map function
	 //hadoop mapper functions implicitly take in the input line by line from the text file
	 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		 
		 //splits line of text into separate words
		 String[] words = value.toString().split("\\s+");
		 
		 //for each word in the line, write it into the context
		 for (String token : words) {
			 
			 word.set(token);
			 context.write(word, one);
		 }
	 }
}
