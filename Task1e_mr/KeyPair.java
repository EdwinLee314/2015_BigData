import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeyPair implements WritableComparable<KeyPair>{
	private Text education;
	private IntWritable balance;
	
	public KeyPair(){
		set(new Text(),new IntWritable());
	}
	
	public KeyPair(String a, int b){
		set(new Text(a),new IntWritable(b));
	}
	
	public void set(Text a, IntWritable b){
		this.education = a;
		this.balance = b;
	}
	
	public Text getEducation(){
		return education;
	}
	
	public IntWritable getBalance(){
		return balance;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		education.write(out);
		balance.write(out); 
	}
		
  @Override
 	public void readFields(DataInput in) throws IOException {
		education.readFields(in);
		balance.readFields(in); 
	}
	
	@Override
	public int hashCode() { 
		return education.hashCode() * 163 + balance.hashCode(); 
	}
	
	@Override
	public int compareTo(KeyPair tp) {
		int cmp = education.compareTo(tp.education);
		if (cmp != 0){ 
			return cmp;
		}
	  return -balance.compareTo(tp.balance);  
  }

	@Override
	public boolean equals(Object o) {
		if (o instanceof KeyPair) {
			KeyPair tp = (KeyPair) o;
			return education.equals(tp.education) && balance.equals(tp.balance);
		}
		return false; 
	}
	
  @Override
  public String toString() {
		return education + ", " + balance; 
	}


}