package preprocessing;


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


public class Preprocessing extends Configured implements Tool{

	
	static HashMap <Long, String> docString = new HashMap <Long, String>();
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Beginningg");  
		//frequencies = WordCount.getFrequencyDict(args);
		int res = ToolRunner.run(new Configuration(), new Preprocessing(), args);
		System.out.println("Ending");  
	}
	

	@Override
	public int run(String[] arg0) throws Exception {
		
	      Configuration configuration = this.getConf();
	     
	      Job job = new Job(configuration, "Preprocessing");
	      job.setNumReduceTasks(1);
	      job.setJarByClass(Preprocessing.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(LongWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("input")); 
	      Path outputPath = new Path("outputPreprocessing");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
	    if (hdfs.exists(outputPath))
	        hdfs.delete(outputPath, true);
	      
	      job.waitForCompletion(true);
	           
	      return 0;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
	      private final static LongWritable ONE = new LongWritable(1);
	      private Text phrase = new Text();
	      private ArrayList<String> stopwords =  ReadCSV.getStopWords();
	    		  
	    		  
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	 HashSet<String> words = new  HashSet<String> ();
	         for (String token: value.toString().split("\\s+")) {
	        	 token = token.replaceAll("[^a-zA-Z0-9 ]", "");
	        	 if(!words.contains(token) && !stopwords.contains(token.toLowerCase())){
	        		 context.write(new Text(token), key);
	        	 }
	        	 words.add(token);
	         }
	         docString.put(key.get(), "");
	        
	      }
	    
	 }

	   public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {
	      
	      
	      TreeSet<WordDocs> wordDocs = new TreeSet<WordDocs>();
	      int linesCount=0;
	      
	      @Override
	      public void reduce(Text key, Iterable<LongWritable> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	  TreeSet<Long> docIds = new TreeSet<Long>();
	          for (LongWritable val : values) {
	        	  docIds.add(val.get());
	          }
	          WordDocs wd = new WordDocs(key.toString(),docIds);
	          wordDocs.add(wd);
	         //context.write(key,new LongWritable(sum));
	      }
	      
	      
	      protected void cleanup(Context ctxt) throws IOException,InterruptedException {
	    	   //we call this fonction once at the end
	           while (!wordDocs.isEmpty()) {
	        	   
	        	   WordDocs biggestWordDoc = wordDocs.pollLast();
	        	   TreeSet<Long> docIds = biggestWordDoc.GetTree();
	        	   
	        	   for(int i = 0;i<docIds.size();i++){
	        		   long docid = docIds.pollFirst();
	        		  docString.put(docid, docString.get(docid) + " " + biggestWordDoc.key);
	        	   }
	        	      
	               
	           }
	           
	           for (Entry<Long, String> entry : docString.entrySet()) {
	        	   Long docid = entry.getKey();
	        	   String doccontent = entry.getValue();
	        	   if (doccontent.matches(".*[a-zA-Z0-9]+.*"))
	        		   ctxt.write(new Text(doccontent.trim()), new Text());
	        	   		linesCount++;
	        	}
	           
	           
	           try{
	               Path pt=new Path("outputPreprocessingLines");
	               FileSystem fs = FileSystem.get(new Configuration());
	               BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));

	               String line="Number of outputs records " +linesCount;         
	               br.write(line);
	               br.close();
	         }catch(Exception e){
	                 System.out.println("File not found");
	         }
	       }
	      
	      
	      
	      
	   }
	

}
