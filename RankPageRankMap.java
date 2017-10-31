import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankPageRankMap extends Mapper<LongWritable, Text, FloatWritable,Text> {
	

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		try {
			context.getInputSplit();
			String rank[]=lineText.toString().split("\t");  
			context.write(new FloatWritable(Float.parseFloat(rank[2])), new Text(rank[0])); //writing the title and page rank values after the iterations
					} catch (Exception e) {
			e.printStackTrace();
		}
	}
}