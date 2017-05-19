package page_rank;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank1 extends Configured implements Tool {
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new PageRank1(), args);
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "TfIdf2");
      job.setJarByClass(PageRank1.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1); 
      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.waitForCompletion(true);
      return 0;
   }
   
   public static class Map extends Mapper<Text, Text, Text, Text> {
     int i = 0;
      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {
    	i++;
    	if(i >4){
    	context.write(key, value);
    	}
      }  
  }
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	   
	  	 HashMap<String, Float> pageRank = new HashMap<String, Float>();  
	  	 protected void setup(Context context) throws IOException, InterruptedException {
	     	 BufferedReader read = new BufferedReader(new FileReader("/home/cloudera/workspace/ecp.lab1/previous_page_rank.txt"));
	     	 String line = null;
	     	 while ((line = read.readLine()) != null){ 
	     		 String[] page_score = line.split("\t"); // add word and value
	     		pageRank.put(page_score[0].toString(), Float.parseFloat(page_score[1]));
	     	 }
	     	 read.close();
		 }
	   
	   
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	 int sum = 0;
	    	 String full_linkto_list = new String();
	    	 for(Text val:values){
	    	 sum++;
	    	 String links_to = val.toString();
	    	 if(sum==1){
	    		 full_linkto_list = pageRank.get(key.toString()) + "," + links_to;
//	    		 full_linkto_list = 1.0 + "," + links_to;

	    		 }
	    	 else{
	    	 full_linkto_list = full_linkto_list + 	"," + links_to;  
	    	 }
	    	 }
	    	 full_linkto_list = full_linkto_list + "," + sum + "";
	         context.write(key, new Text(full_linkto_list));
	      }
	   }
	}



