package aggregator2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Aggregator2Reducer extends Reducer<Text,LongWritable,Text,LongWritable>{
	
	private final static LongWritable value = new LongWritable();
	private final static Text newKey = new Text();
	
	public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException,InterruptedException{
		StringBuilder s = new StringBuilder(key.toString());
		s.append(" : ");
		newKey.set(s.toString());
		int max = 0;
		for(LongWritable val : values) {
			if ((int)val.get()>max) {
				max=(int)val.get();
				value.set(val.get());
			}

		}
		context.write(newKey, value);
	}
}
