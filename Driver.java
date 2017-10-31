import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		boolean success=TitleCount(args);
		success=InitialPageRank(args);
		for(int i=0;i<10;i++)  //Creating folders for the 10 iterations
		{
			String input=args[1]+"FinalPageRank_"+i;
			String output=args[1]+"FinalPageRank_"+(i+1);
			success=FinalPageRank(input,output);}
		String input=args[1]+"FinalPageRank_10";
		String output=args[1]+"sorted_output";
		success=SortPageRank(input,output);
		success=deleteFolders(new File(args[1]));
		return success? 0:1 ;
	
	}
		
	
public boolean TitleCount(String[] args) throws Exception {          //job for Counting number of titles
	Job job1 = Job.getInstance(getConf(), "Calculate Total Titles");
	job1.setJarByClass(this.getClass());
	// Instantiating it's Mapper, and Reducer classes
	job1.setMapperClass(TitleCountMap.class);
	job1.setReducerClass(TitleCountReduce.class);
	// Instantiating input and output paths for Job 2
	FileInputFormat.addInputPath(job1, new Path(args[0]));
	FileOutputFormat.setOutputPath(job1, new Path(args[1]
			+ "Title_Count"));
	// Instantiating corresponding output classes
	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(IntWritable.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(IntWritable.class);
	boolean success = job1.waitForCompletion(true);
	
	return success;
}


	
public boolean InitialPageRank(String[] args) throws Exception {
	//Configuration object containing Size of the input from Job 1
	Configuration conf = new Configuration();
	double num_of_titles = 0;
	BufferedReader input = new BufferedReader(new FileReader(args[1]
			+ "Title_Count/part-r-00000"));
	String str;
	while ((str = input.readLine()) != null) {
		String splitted[] = str.split("\t");
		num_of_titles = Double.parseDouble(splitted[1]);
		break;
	}
	input.close();
	conf.setDouble("length", num_of_titles);  //getting the number of titles in the input
	
	

	Job job2 = Job.getInstance(conf, "Calculate Initial Page Rank"); //job for calculating the initial page ranks
	job2.setJarByClass(this.getClass());
	// Instantiating it's Mapper, and Reducer classes
	job2.setMapperClass(InitialPageRankMap.class);
	job2.setReducerClass(InitialPageRankReduce.class);
	// Instantiating input and output paths for Job 2
	FileInputFormat.addInputPath(job2, new Path(args[0]));
	FileOutputFormat.setOutputPath(job2, new Path(args[1]
			+ "FinalPageRank_0"));
	// Instantiating corresponding output classes
	job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(Text.class);
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
	boolean success = job2.waitForCompletion(true);
	return success;
}
public boolean FinalPageRank(String input, String output) throws Exception {  //job for calculating the page ranks after iterations
Job job3 = Job.getInstance(getConf(), "Calculate Final Page Rank");
job3.setJarByClass(this.getClass());
// Instantiating it's Mapper, and Reducer classes
job3.setMapperClass(FinalPageRankMap.class);
job3.setReducerClass(FinalPageRankReduce.class);
// Instantiating input and output paths for Job 3
FileInputFormat.addInputPath(job3, new Path(input));
FileOutputFormat.setOutputPath(job3, new Path(output));
// Instantiating corresponding output classes
job3.setMapOutputKeyClass(Text.class);
job3.setMapOutputValueClass(Text.class);
//job3.setOutputKeyClass(Text.class);
//job3.setOutputValueClass(Text.class);
boolean success = job3.waitForCompletion(true);
return success;
}
public boolean SortPageRank(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
	Job job4 = Job.getInstance(getConf(), "sort"); //job for sorting the page rank in descending order
	job4.setJarByClass(this.getClass());
	// Instantiating it's Mapper, and Reducer classes
	job4.setMapperClass(RankPageRankMap.class);
	job4.setReducerClass(RankPageRankReduce.class);
	job4.setSortComparatorClass(comp.class);
	// Instantiating input and output paths for Job 4
	FileInputFormat.addInputPath(job4, new Path(input));
	FileOutputFormat.setOutputPath(job4, new Path(output));
	// Instantiating corresponding output classes
	job4.setMapOutputKeyClass(FloatWritable.class);
	job4.setMapOutputValueClass(Text.class);
	job4.setOutputKeyClass(Text.class);
	job4.setOutputValueClass(FloatWritable.class);
	boolean success = job4.waitForCompletion(true);
	return success;
}
// Deleting all the folders except the final output folder
		private boolean deleteFolders(File output) {
			boolean success = false;
			if (output.isDirectory()) {
				String[] files = output.list();
				for (int i = 0; i < files.length; i++) {
					if(!(files[i].equals("sorted_output")))
					success = deleteFolders(new File(output, files[i]));
					if (!success) {
						return false;
					}
				}
			}
			return output.delete();
		}
	
public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Driver(), args);
	System.exit(res);
}
}

 class comp extends WritableComparator{ //Comparator used for sorting the page ranks in descending order
	protected comp(){
		super(FloatWritable.class,true);
		
	}
	public int compare(WritableComparable x,WritableComparable y){
		FloatWritable f1=(FloatWritable) x;
		FloatWritable f2=(FloatWritable) y;
		int result=f1.compareTo(f2);return result*-1;       
	}
}
