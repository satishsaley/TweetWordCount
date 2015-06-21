package tweet.count;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err.println("Incorrect number of arguments.  Expected: input output");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(Driver.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(TweetReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception{
		Driver driver = new Driver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}

}
