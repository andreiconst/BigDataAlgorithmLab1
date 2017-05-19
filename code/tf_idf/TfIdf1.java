package ecp.lab1;


import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TfIdf1 extends Configured implements Tool {
   
	public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new TfIdf1(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
	Job job = new Job(getConf(), "InvertedIndex");
      job.setJarByClass(TfIdf1.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      
      job.setNumReduceTasks(1);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private String file_name = new String();
      private Text word = new Text();
      private final static IntWritable ONE = new IntWritable(1);


      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  file_name = ((FileSplit) context.getInputSplit()).getPath().getName();

    	  String line = value.toString();
          line = line.toLowerCase();
          for (String token: value.toString().split("\\s+")) {
          		token = token.replaceAll("[^A-Za-z]","");
          		if(!token.isEmpty()){
	         	  	token = token.toLowerCase();
	         	  	token = token + "," + file_name;
	         	  	word.set(token);
	         	  	context.write(word, ONE);
          		}
           }
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	         int sum = 0;
	         for (IntWritable val : values) {
	            sum += val.get();
	         }
	        context.write(key, new IntWritable(sum));
	      }
	   }
}

