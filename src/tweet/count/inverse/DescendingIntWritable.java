package tweet.count.inverse;

import org.apache.hadoop.io.IntWritable;

public class DescendingIntWritable extends IntWritable{

	public DescendingIntWritable(){
		
	}
	public DescendingIntWritable(int value){
		super(value);
	}
	/*
	 * Reverse the compareTo ouput to have desending elements
	 */
	@Override
	public int compareTo(IntWritable o) {
		if(o==null)
			return -1;
		if(this.get() > o.get())
			return -1;
		else if(this.get() < o.get())
			return 1;
		return 0;
	}

}
