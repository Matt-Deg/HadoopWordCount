package wordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//configuration object for the mapreduce job
	    Configuration conf = new Configuration();
	    
	    //creates instance of a mapreduce job
	    Job job = Job.getInstance(conf, "word count");
	    
	    //specifies which class has the driver code
	    job.setJarByClass(Driver.class);
	    
	    //sets the mapper class
	    job.setMapperClass(MapperWC.class);
	    
	    //sets the reducer class
	    job.setReducerClass(ReducerWC.class);
	    
	    //sets data types for key-value pairs
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    //sets input and output file directories in command line
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    //exits the program when the job is done
	    //otherwise, program will keep running infinitely
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
