import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TitleCountReduce extends Reducer<Text,IntWritable, Text, IntWritable> {

public void reduce(Text title, Iterable<IntWritable> countvalue, Context context)throws IOException, InterruptedException {
	int sum=0;
	for (IntWritable value : countvalue) {
		sum=sum+value.get();    //summing the titles 
}context.write(title,new IntWritable(sum));    
}
}
