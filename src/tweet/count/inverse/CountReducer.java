package tweet.count.inverse;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<IntWritable,Text,IntWritable,Text>{

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text value : values)
		{
			context.write(key,value);
		}
	}

}
