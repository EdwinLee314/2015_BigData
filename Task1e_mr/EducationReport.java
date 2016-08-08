import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

// The driver class
public class EducationReport
{
  public static void main(String[] args) throws Exception
  {
    if(args.length == 2)
    {
      // Instantiate a Job object for your job's configuration
      Job job = new Job();
      
      // Tell Hadoop which Jar file contains our code
      job.setJarByClass(EducationReport.class);
      
      // Set a job name which will appear in logs
      job.setJobName("Task1e(EducationReport)");

      // Set input and output names from arguments
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // Set mapper and reducer classes
      job.setMapperClass(ReportMapper.class);
      job.setReducerClass(SortReducer.class);

      // Set key and value types for the mapper output
      job.setMapOutputKeyClass(KeyPair.class);
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
      System.out.println("Usage: Task1e(EducationReport) <input dir> <output dir>");
    }
  }
}
