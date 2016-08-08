import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// The reducer class
public class TopOneCombiner extends Reducer<Text, Text, Text, Text>
{
	private IntWritable bigest = new IntWritable(0);
	private Text aimDataKey = new Text("");
	private Text aimDataPast = new Text("");
	private Text aimDataLatest = new Text("");
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException
	{
		
		int past = 0;
		int latest = 0;
		Text pastVa= new Text("");
		Text lateVa= new Text("");
		for (Text value : values){
			
			String va = value.toString();
			String time = va.substring(0,1);
			String amount = va.substring(1);
			
			if(time.equals("p")){
				past = Integer.parseInt(amount);
				pastVa = new Text(value);
			}
			else if(time.equals("l")){
				latest = Integer.parseInt(amount);
				lateVa = new Text(value);
			}
		}
		
		if(latest != 0 && past != 0){
			int result = latest - past;
			if(result > bigest.get()){
				bigest.set(result);
				aimDataKey.set(key);
				aimDataPast.set(pastVa);
				aimDataLatest.set(lateVa);
			}
		}
		else{
			if(latest == 0 && past != 0){
				context.write(new Text(key), new Text(pastVa));
			}
			if(latest != 0 && past == 0){
				context.write(new Text(key), new Text(lateVa));
			}
		}
	}
	
	public void cleanup(org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context){
		try {
			context.write(aimDataKey, aimDataPast );
			context.write(aimDataKey, aimDataLatest );
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}

