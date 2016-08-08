import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// The combiner class
public class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
{
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException{
    int jobCount = 0;
    
    // Iterate over the values passed to us by the mapper
    for (IntWritable value : values)
    {
      // Add the value to the job count counter for this key.
      jobCount += value.get();
    }
    
    // Emit the job accompanied by its final count
    context.write(key, new IntWritable(jobCount));
  }
}
