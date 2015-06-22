package tweet.count.inverse;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, DescendingIntWritable,Text>{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);
		while(st.hasMoreTokens()){
			String word = st.nextToken().trim();
			String numberStr = st.nextToken().trim();
			int num ;
			try{
				num = Integer.parseInt(numberStr);
			}catch(NumberFormatException e){
				return;
			}
			
			context.write(new DescendingIntWritable(num), new Text(word));
		}
		
	}
	

}
