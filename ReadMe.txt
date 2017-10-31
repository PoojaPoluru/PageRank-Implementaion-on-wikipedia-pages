 									
					PageRank of Wikipedia pages



Compiler:Java               CompilerLanguage 	:Java                              IDE :Eclipse
-------------------------------------------------------------------------------------------------------

Compilation and execution using the TerminalStep to create folder in HDFS: Commands to create input folder in hdfs
1.sudo su hdfs
2.Hadoop fs -mkdir /user/cloudera
3.Hadoop fs -chown cloudera /user/cloudera
4.Exit
5.Sudo su cloudera
6.Hadoop fs -mkdir /user/cloudera/input 

Adding files to HDFS

Step 1: Place the input files in HDFS.Command: hadoop fs -put < CanterburyFile folder Path> <Path in HDFS>
Example: Hadoop fs -put /home/cloudera/Downloads/cantrbry/Canterbury/ /user/cloudera/input

---------------------------------------------------------------------------------------------------------------

Driver Class:It has configurations to run jobs

Title Count: This MapReduce jobs helps in writing the number of titles in an output file and takes the value of number of titles,N from that file.



Initial PageRank:This Job gets the value of N from job1 and gives all the titles a initial page rank value of 1/N.It also separates the titles and the outlinks and prints them.



FinalPageRank: This MapReduce job,gets the 10 iterations done to compute the final page rank of the pages.The dangling node does not have any outgoing links and the count of it is not given.It also creates 10 folders for all the 10 iterations.Red links will also be eliminated in this job.




sortPageRank:This MapReduce job cleans the output obtained in the iteraration 10 and puts it in a different folder.It is sorted in an descending order.

deleteFolders:This method deletes all the iteration output folders and retains only the sorted output folder

--------------------------------------------------------------------------------------------------------------



Execution :


Step 1: Compile java file

mkdir -p build

Java -cp /usr/lib/hadoop/:/usr/lib/hadoop-mapreduce/ path/ -d build -Xlint

Path here would be the Driver.java path.


So in the place of path/ we give    /home/cloudera/workspace/training/src/wikipedia/org/Driver.java

------------------


Step 2: Generating the JAR

jar -cvf -C build/ .



We give: jar -cvf Driver.jar -C build/ .

--------------------


Step 3: Running the JAR generated

Hadoop jar

Example: Hadoop jar PageRank.jar wikipedia.org.Driver /user/cloudera/input/ /user/cloudera/output/


To Run the program again Delete the output Folder:


To delete output folder: Hadoop fs -rm -r /path

In place of path put /home/cloudera/output/

sorted_output is the folder containing output. 
It has page titles and pageRank values in decreasing order.

To execute the program again delete the output folder and re-run it.
 
