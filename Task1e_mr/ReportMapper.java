import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The mapper class
public class ReportMapper extends Mapper<LongWritable, Text, KeyPair, Text>
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
		String job = "xx";
		String marital = "xx";
		String education = "xx";
		String loan = "xx";
		int balance = 1234;
		
    for (String word : line.split(";")){
			count = count + 1;
			if (count == 2){
				job = word;
			}	
			else if(count == 3){
				marital = word;
			}
			else if(count == 4){		
				education = word;
			}
			else if(count == 6){
				balance = Integer.parseInt(word);
			}
			else if(count == 8){
				loan = word;
				KeyPair rKey = new KeyPair(education, balance);
				String rValue = job + ", " + marital + ", " + loan; 
				// Emit the job
        context.write( rKey ,new Text(rValue));
				count = 0;
				break;
			}  
    }
		
  }
}
