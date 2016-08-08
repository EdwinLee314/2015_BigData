import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The mapper class
public class BalanceMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
  // The map method runs once for each line of text in the input file
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
  {
    // Get the line as a simple Java String
    String line = value.toString();

    // Split the line into words
		int count = 0;
    for (String word : line.split(";"))
    {
			count = count + 1;
      if (count == 6)
      {
				long balance = Long.parseLong(word);
				String level;
				if(balance >= 1501L){
					level = "High";
				}
				else if(balance <= 500L){
					level = "Low";
				}
				else{
					level = "Medium";
				}
        // Emit the job accompanied by an intermediate count of 1
        context.write(new Text(level), new IntWritable(1));
				count = 0;
				break;
      }
    }
  }
}
