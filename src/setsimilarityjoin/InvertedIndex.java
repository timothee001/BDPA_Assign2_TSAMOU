package setsimilarityjoin;


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


public class InvertedIndex extends Configured implements Tool{

	static double thresold =0.5;
	static HashMap<Long,String> docs = new HashMap<Long,String>();

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Beginningg");  
		//frequencies = WordCount.getFrequencyDict(args);
		int res = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
		System.out.println("Ending");  
	}
	
	public static int getNumberOfWordsToKeep(String document){
		
		int wordsNumber = countWords(document);
		double td = Math.ceil(thresold * wordsNumber);
		return wordsNumber - (int)td  +1;
	}
	
	public static int countWords(String s){

	    int wordCount = 0;

	    boolean word = false;
	    int endOfLine = s.length() - 1;

	    for (int i = 0; i < s.length(); i++) {
	        // if the char is a letter, word = true.
	        if (Character.isLetter(s.charAt(i)) && i != endOfLine) {
	            word = true;
	            // if char isn't a letter and there have been letters before,
	            // counter goes up.
	        } else if (!Character.isLetter(s.charAt(i)) && word) {
	            wordCount++;
	            word = false;
	            // last word of String; if it doesn't end with a non letter, it
	            // wouldn't count without this.
	        } else if (Character.isLetter(s.charAt(i)) && i == endOfLine) {
	            wordCount++;
	        }
	    }
	    return wordCount;
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
	     
	      Job job = new Job(configuration, "InvertedIndex");
	      job.setNumReduceTasks(1);
	      job.setJarByClass(InvertedIndex.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(LongWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path("inputSimilarity")); 
	      Path outputPath = new Path("outputInvIndex");
	      FileOutputFormat.setOutputPath(job, outputPath);
	      FileSystem hdfs = FileSystem.get(getConf());
	    if (hdfs.exists(outputPath))
	        hdfs.delete(outputPath, true);
	      
	      job.waitForCompletion(true);
	           
	      return 0;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
	  
	     	
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	
	    	  docs.put(key.get(), value.toString());
	    	  String[] parts = value.toString().split("\\s+");
	    	//  System.out.println(value + " : "+getNumberOfWordsToKeep(value.toString()));
	    	  String keytoemit ="";
	    	  for(int i =0;i<getNumberOfWordsToKeep(value.toString());i++){
	    		  
	    		  //System.out.println(parts[i]);
	    		  keytoemit=keytoemit+parts[i]+" ";
	    	  } 
	    	  
	    	  context.write(new Text(keytoemit.trim()), key);
	      }
	      
	    
	 }

	   public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {
	      
		  
	      @Override
	      public void reduce(Text key, Iterable<LongWritable> values, Context context)
	              throws IOException, InterruptedException {
	    	  
	    	  ArrayList<Long> docsId = new ArrayList<Long>();
	    	  //System.out.println(key +" : ");
	    	  
	    	  
	    	  
	    	  for (LongWritable entry : values) {
	    		    docsId.add(entry.get());   
	    	  }
	    	  
	    	  for(int i=0;i<docsId.size();i++){
	    		  
	    		  for(int j=i;j<docsId.size();j++){
	    			  
	    			  if(j!=i){
	    				  
	    				  long iddoc1 = docsId.get(i);
	    				  
	    				  long iddoc2 =  docsId.get(j);
	    				  
	    				  
	    				  
	    				  Double sim = InvertedIndex.similarity(docs.get(iddoc1),docs.get(iddoc2));
	    		    	  if(sim>=InvertedIndex.thresold)
	    		    		  context.write(new Text("(d"+iddoc1+",d"+iddoc2+")"), new Text(sim.toString()));
	    				  
	    				  
	    			  }
	    			  
	    		  }
	    		  
	    		  
	    	  }
	    	  
	  
	      }
	    	  
	       
	      
	      

	           
	      
	   }
	

}
