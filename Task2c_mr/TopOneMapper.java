import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The mapper class
public class TopOneMapper extends Mapper<LongWritable, Text, Text, Text>
{
	// The query word to search for
	private String PAST_MONTH = "";
	private String LATEST_MONTH = "";

  @Override
	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
	{
		String line = value.toString();
		PAST_MONTH = context.getConfiguration().get("past_month");
		LATEST_MONTH = context.getConfiguration().get("latest_month");
		int count = 0;
		String month = "";
		String tag = "";
		String amount = "";
		boolean emitFlag = false;
		for(String word : line.split("\t")){
			count = count + 1;
			//month
			if(count == 2){
				if(word.equals(PAST_MONTH)){
					month = "p";
				}
				else if(word.equals(LATEST_MONTH)){
					month = "l";
				}
				else{
					break;
				}
			}
			//count
			if(count == 3){
				int tp = Integer.parseInt(word); 
				if(tp == 0 && month.equals("")){
					emitFlag = false;
				}else{
					emitFlag = true;
				}
				amount = month + word;
			}
			//tag
			if(count == 4 ){
				tag = word;
				if(emitFlag == true){
					context.write(new Text(tag) ,new Text(amount));
				}
			}
		}
	}
}