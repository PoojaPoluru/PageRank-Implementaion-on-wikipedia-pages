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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class InitialPageRankMap extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
		String get_title[] = new String[2];
		
		String line_String = lineText.toString();
		// Retrieving data between <title> and </title> tags
		if (line_String.contains("<title>") && line_String.contains("</title>")) {
			int data_start = lineText.find("<title>");
			int data_end = lineText.find("</title>", data_start);
			data_start = data_start + 7;
			int x = data_end - data_start;
			if (data_start != data_end && data_end > data_start) {
				get_title[0] = Text.decode(lineText.getBytes(), data_start, x);
			} else
				get_title[0] = " ";
		} else
			get_title[0] = " ";

		// Retrieving data between <text> and </text> tags
		try{
			boolean check = true;
		String outlinks = getText(lineText);
		if (outlinks != null)
			context.write(new Text(get_title[0]),new Text(outlinks));
		//System.out.println(get_title[0]+ " "+outlinks);
		
			//System.out.println("out" + outlinks);
	}
	catch (Exception e)
		{
		e.printStackTrace();
		}
		}
//Code to extract outgoing links
	public String getText(Text line) {
		Pattern linkPat = Pattern.compile("\\[\\[.*?]\\]");
		String url = "";
		String output = "";
		String line_String = line.toString();
		Matcher m = linkPat.matcher(line_String);
		
		// extract outgoing links
		while (m.find()) { // loop on each outgoing link
			url = m.group().replace("[[", "").replace("]]", ""); // drop the brackets and any nested one if any
			if (!url.isEmpty()) 
			{
				
				if(output.equals("")) //If there is just one outgoing link,
				output=output+url;
				else
					output=output+";"+url; //Put a delimiter between the outlinks
				} 
			
		}
		return output; //Returning the appropriate output based on the above statements 
		}
		}
