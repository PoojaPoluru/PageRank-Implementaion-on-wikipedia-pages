import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class InitialPageRankReduce extends Reducer<Text, Text, Text, Text> {
public void reduce(Text word, Iterable<Text> value, Context context)throws IOException, InterruptedException {
	
	Configuration conf=context.getConfiguration();
	double Length_of_doc=conf.getDouble("length",0.0d); //getting the length of the document from the driver that is the number fo files N
	Double Initial_Rank=1/Length_of_doc;                //Initial Page rank of all the pages is set to 1/N
	String output = "";
	output = output;
	
		for (Text count : value) {
			if(output.equals(""))
			output=output+count;     
			else
				output=output+";"+count;  //putting a delimiter between the outlinks 
		
}if(!(word.equals("")))   //if the word is not null
	context.write(word, new Text(output+"\t"+ String.valueOf(Initial_Rank)));   
}

}

