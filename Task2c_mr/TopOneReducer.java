import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// The reducer class
public class TopOneReducer extends Reducer<Text, Text, Text, Text>
{
	private IntWritable bigest = new IntWritable(0);
	private Text aimData = new Text("");
 
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException
	{
		
		int past = 0;
		int latest = 0;
		
		for (Text value : values){	
			String va = value.toString();
			String time = va.substring(0,1);
			String amount = va.substring(1);
			
			if(time.equals("p")){
				past = Integer.parseInt(amount);
			}
			else if(time.equals("l")){
				latest = Integer.parseInt(amount);
			}
		}
		
		if(latest != 0 && past != 0){
			
			int result = latest - past;
			
			if(result > bigest.get()){
				bigest.set(result);
				aimData.set(key + ", " + past + ", " + latest);
			}
		}

	}
	
	public void cleanup(org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context){
		try {
			context.write(aimData, new Text(""));
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}

