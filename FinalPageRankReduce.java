import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalPageRankReduce extends Reducer<Text,Text, Text, Text> {

public void reduce(Text title, Iterable<Text> countvalue, Context context)throws IOException, InterruptedException {
	double sum=0;int temp=0;String outlinks="";
	if(title.equals("")){  //If there is no title 
		return;				
	}
	for(Text x:countvalue){               
		String list[]=x.toString().split("\t");   //split the values obtained from mapper
		if(list[0].equals("flag"))                //if there is a flag in the value
			{ 
			temp=1;                             //set of temp variable by 1
			if(list.length==2)                   //if the length of the splitted list is 2
			outlinks=list[1];                    //set the outlinks to 1
			}
		else
		{
			if(!(list.length==2))                 //else if there is a dangling node continue
				continue;
			else{
				if(list[1].equals("poo")&&title.toString().equals("poo"))
				{
					System.out.println("no outlinks");break;
				}
				else
			{sum=sum+(Double.parseDouble(list[0])/Double.parseDouble(list[1]));} }  //sum will be equal to the pagerank divided by the number of outlinks
		}	
	}
	if(temp==1)        //if there is a flag
	{
		double result=0.85*sum+0.15;  //pagerank will be computed based on the formula
		
		context.write(new Text(title),new Text(outlinks+"\t"+result));    //initial page rank for the individual pages is obtained
	}
	
	

}
}