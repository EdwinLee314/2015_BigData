import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// The reducer class
public class SortReducer extends Reducer<KeyPair, Text, Text, Text>
{
  // The reduce method runs once for each key received after the shuffle phase
  @Override
  public void reduce(KeyPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException
  {
    // Iterate over the values passed to us by the mapper
    for (Text value : values)
    {
				// Emit the job accompanied by its final count
				context.write(new Text(key.toString() + ", " + value ),new Text(""));
    }
    
   
  }
}
