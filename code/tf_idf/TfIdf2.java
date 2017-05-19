package ecp.lab1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TfIdf2 extends Configured implements Tool {
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new TfIdf2(), args);
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "TfIdf2");
      job.setJarByClass(TfIdf2.class);
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
     
      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {
    	String new_value = new String();
    	new_value = value.toString();
    	String[] couple = key.toString().split(",");
    	String document = new String();
    	document = couple[1] + "," +new_value;
    	context.write(new Text(couple[0]), new Text(document));

      }
  }
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	   
	  	 HashMap<String, Integer> wordCount = new HashMap<String, Integer>();  
	  	 protected void setup(Context context) throws IOException, InterruptedException {
	     	 BufferedReader read = new BufferedReader(new FileReader("/home/cloudera/workspace/ecp.lab1/wordsperdoc.txt"));
	     	 String line = null;
	     	 while ((line = read.readLine()) != null){ 
	     		 String[] word_couple = line.split("\t"); // add word and value
	     		 wordCount.put(word_couple[0].toString(), Integer.parseInt(word_couple[1]));
	     	 }
	     	 read.close();
		 }
	   
	   
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	 for(Text val:values){
	    	 String[] document = val.toString().split(",");
	    	 String word_count = new String();
	    	 String word_provenance = new String();
	    	 word_provenance = document[0] + ","+ key.toString();
	    	 word_count = document[1] + "," + wordCount.get(document[0]);	    	 
	         context.write(new Text(word_provenance), new Text(word_count));
//	    	 context.write(new Text(key.toString()), new Text(values.toString()));
	    	 }
	      }
	   }
	}


