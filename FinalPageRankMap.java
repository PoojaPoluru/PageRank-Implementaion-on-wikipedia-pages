import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalPageRankMap extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		try {
			context.getInputSplit();
			String line = lineText.toString(); //converting line to string
			String split[] = {""};         
			
			split = line.split("\t");          //split the line based on the tab space
			String page_title = split[0];      //obtaining page title value
			String page_rank_value = split[2]; //obtaining page rank value
			if(split[1].equals("")){              //splittled[1] is the outlinks and if there is no outlinks
				context.write(new Text(page_title), new Text("flag"+"\t"+""));        // we put a flag there with the title and
				context.write(new Text("poo"), new Text( page_rank_value+"\t"+"poo")); //give the page rank value
				return;
			}
			else{
				if(split[1].contains(";")){   //if the title contains outlinks
					String outlinks[]=split[1].split(";");       //get the outlinks spitted based on delimiter
					context.write(new Text(page_title), new Text("flag"+"\t"+split[1]) );     
					for(String x:outlinks)
						context.write(new Text(x), new Text(split[2]+"\t"+outlinks.length));   //we are giving the outlinks the page rank and the length of outlinks
					}
				else{ 
					context.write(new Text(split[1]), new Text(split[2]+"\t"+1));  //if there is only one outlink,outlink and the corresponding page rank and number of outlinks is 1
				context.write(new Text(page_title), new Text("flag"+"\t"+split[1]) );}   //page title and flag and outlinks.
				
			}
				} catch (Exception e) {
			e.printStackTrace();
		}
	}
}