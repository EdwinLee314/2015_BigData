import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

// The driver class
public class TopOne
{
  public static void main(String[] args) throws Exception
  {
    if(args.length == 4)
    {
		// Instantiate a Job object for your job's configuration
		Configuration conf = new Configuration();
		conf.set("past_month", args[2]);
		conf.set("latest_month", args[3]);

		Job job = new Job(conf);
		job.setJarByClass(TopOne.class);
		job.setJobName("Task2c(most increased tweets)");

		// Set input and output paths from command-line arguments
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Set mapper and reducer classes
		job.setMapperClass(TopOneMapper.class);
		job.setReducerClass(TopOneReducer.class);
		// Set combiner classes
		job.setCombinerClass(TopOneCombiner.class);
		// Set key and value types for the mapper output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set key and value types for the reducer (final) output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	  
		// Start the MapReduce job and wait for it to finish
		boolean success = job.waitForCompletion(true);
		System.out.printf("Job %s.", success ? "succeeded" : "failed");
    }
    else
    {
      System.out.println("Usage: Task2c(most increased tweets) <input dir> <output dir> <firstdate> <seconddate>");
    }
  }
}
