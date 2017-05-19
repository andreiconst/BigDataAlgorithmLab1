package page_rank;


import java.io.IOException;
import java.util.Arrays;
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

public class PageRank2 extends Configured implements Tool {
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new PageRank2(), args);
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), "PageRank2");
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

    	  String from_page = key.toString();
    	  String[] total_links = value.toString().split(",");
    	  for(int i=1; i<(total_links.length-1); i++){
    		  String document = from_page + "," + total_links[0] + "," + total_links[total_links.length-1];
    	      context.write(new Text(total_links[i]), new Text(document));
    	  }
	      context.write(new Text(from_page), new Text("the end"));
    	}  
      }
   
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	   
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	 
	    	 double page_rank = 0;
	    	 
	    	 for(Text val:values){
	    	 String[] links_from = val.toString().split(",");
		    	 if(!links_from[0].equals("the end")){
		    	 page_rank += 0.85 * ((double) Float.parseFloat(links_from[1]) / (double) Float.parseFloat(links_from[2])) ;
		    	 }
	    	 }
			 page_rank += (double) 0.15; 
	         context.write(key, new Text(page_rank+""));
	      }
	   }
	}
   


  
