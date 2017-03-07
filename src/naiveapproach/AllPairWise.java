package naiveapproach;


import java.awt.List;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ReadCSV.ReadCSV;
import preprocessing.WordCount;


public class AllPairWise extends Configured implements Tool{

	static double thresold =0.8;
	
	
	static HashMap <Long, String> docString = new HashMap <Long, String>();
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Beginningg");  
		//frequencies = WordCount.getFrequencyDict(args);
		int res = ToolRunner.run(new Configuration(), new AllPairWise(), args);
		System.out.println("Ending");  
	}
	
	public static double similarity(String s1, String s2) {
        
		Set<String> intersect = new HashSet<>();
	    Set<String> union = new HashSet<>();
	       
	    Set<String> set1 =   new HashSet<String>();
	    Set<String> set2 =   new HashSet<String>();
	    
	    String[] parts = s1.split(" +");
	    for(String p : parts) {
	    	set1.add(p);
	    }
	    
	    String[] parts2 = s2.split(" +");
	    for(String p : parts2) {
	    	set2.add(p);
	    }
	 
	    
	   intersect.clear();
	   intersect.addAll(set1);
	   intersect.retainAll(set2);
	   union.clear();
	   union.addAll(set1);
	   union.addAll(set2);
	   return (double)intersect.size()/(double)union.size();
    }

	

	@Override
	public int run(String[] arg0) throws Exception {
		
	      Configuration configuration = this.getConf();
	     
	      Job job = new Job(configuration, "AllPairWiseJob");
	      job.setNumReduceTasks(1);
	      job.setJarByClass(AllPairWise.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("inputSimilarity")); 
	      Path outputPath = new Path("outputAllPairWise");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
	    if (hdfs.exists(outputPath))
	        hdfs.delete(outputPath, true);
	      
	      job.waitForCompletion(true);
	           
	      return 0;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	      private final static LongWritable ONE = new LongWritable(1);
	      private Text phrase = new Text();
	     
	      ArrayList<Doc> map = new ArrayList<Doc>();
	     	
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	Doc currentDoc = new Doc(key.get(),value.toString());
	    	
	    	int mapSize = map.size();
	    	for(int i=mapSize-1;i>0;i--){
	    		context.write(new Text(currentDoc.GetId()+"_"+map.get(i).GetId()), new Text(currentDoc.GetContent()));
	    		context.write(new Text(currentDoc.GetId()+"_"+map.get(i).GetId()), new Text(map.get(i).GetContent()));	
	    	}
	    	
	    	map.add(currentDoc);
	    	
	    	
	      }
	      
	    
	 }

	   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	      
	     
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	  Text s1 = new Text();
	    	  Text s2 = new Text();
	    	  
	    	  int count = 0;
	    	  for (Text val : values) {
	        	  if(count==0){
	        		  s1.set(val);
	        	  }else{
	        		  s2.set(val);
	        	  }
	        	  count++;
	          }
	    	  
	    	  
	    	  String [] keys = key.toString().split("_");
	    	  Double sim = AllPairWise.similarity(s1.toString(), s2.toString());
	    	  if(sim>=AllPairWise.thresold)
	    		  context.write(new Text("(d"+keys[0]+",d"+keys[1]+")"), new Text(sim.toString()));
	    	  
	    	  
	    	  
	         //context.write(key,new LongWritable(sum));
	      }
	      
	      
	      protected void cleanup(Context ctxt) throws IOException,InterruptedException {
	    	   //we call this fonction once at the end
	          
	       }
	           
	      
	   }
	

}
