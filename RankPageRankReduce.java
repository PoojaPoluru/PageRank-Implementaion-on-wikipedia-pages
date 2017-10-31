import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RankPageRankReduce extends Reducer<FloatWritable, Text, Text, FloatWritable> {

	public void reduce(FloatWritable value, Iterable<Text> lineText, Context context)
			throws IOException, InterruptedException {

//System.out.println("reuddedewfaeufhiufhnia")
			for(Text x:lineText)	
		context.write(x,value);   //Writing values from mapper to output
	}
}