import java.io.File;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TitleCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
        String get_title[] = new String[2];
		
        //Getting the title count for the value of N
		String line_String = lineText.toString();
		if (line_String.contains("<title>") && line_String.contains("</title>")) {
			int data_start = lineText.find("<title>");          
			int data_end = lineText.find("</title>", data_start);
			data_start = data_start + 7;                         //Start the data from the 7th position in the line
			int x = data_end - data_start;
			if (data_start != data_end && data_end > data_start) {             //If there is a title between the tags
				get_title[0] = Text.decode(lineText.getBytes(), data_start, x);  //get the text from the data start to 
			} else
				get_title[0] = " ";
		} else
		get_title[0] = " ";
		if(get_title[0].equals(" "))   //If there is no title
		context.write(new Text("pooja"), new IntWritable(0)); //count is 0
		else
			context.write(new Text("pooja"), new IntWritable(1)); // else count is 1
	}
}
