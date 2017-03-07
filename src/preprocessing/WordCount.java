package preprocessing;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
	
	static HashMap <String, Integer> frequencies = new HashMap<String, Integer>();
	
	
	public static HashMap <String, Integer> getFrequencyDict(String[] args) throws Exception {

	      int res = ToolRunner.run(new Configuration(), new WordCount(), args);
	      return frequencies;
	}
	
   

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration configuration = this.getConf();
     
      Job job = new Job(configuration, "WordCount");
      job.setNumReduceTasks(1);
      job.setJarByClass(WordCount.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path("input"));
      FileOutputFormat.setOutputPath(job, new Path("outputWordCount"));
      FileSystem hdfs = FileSystem.get(getConf());
 	  if (hdfs.exists(new Path("outputWordCount")))
 	      hdfs.delete(new Path("outputWordCount"), true);
      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
         for (String token: value.toString().split("\\s+")) {
        	token = token.replaceAll("[^a-zA-Z0-9 ]", "");
            word.set(token);
            context.write(word, ONE);
         }
      }
   }

  
   
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	   
	  HashMap <String, Integer> freq = new HashMap<String, Integer>();
	      
	   
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         
         String newKey = key.toString();
         
         freq.put(newKey, sum);
         
         context.write(key, new IntWritable(sum));
      }
      
      protected void cleanup(Context ctxt) throws IOException, InterruptedException {
    	  WordCount.frequencies = (freq);
      }
   }
}
