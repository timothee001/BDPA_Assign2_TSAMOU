package ReadCSV;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ReadCSV {

	public static ArrayList<String> getStopWords(){
		ArrayList<String> stopwords= new ArrayList<String>();
		
		String csvFile = "stopwords.csv";
        String line = "";
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] words = line.split(cvsSplitBy);

              
                stopwords.add(words[0].toLowerCase());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
		//System.out.println(stopwords);
		return stopwords;
	}
	public static void main(String[] args){
		
		String s1 = "Salut ca va et toi";
		String s2= "Salut ca toi";
		
		Set<String> intersect = new HashSet<>();
	    Set<String> union = new HashSet<>();
	       
	    Set<String> set1 =   new HashSet<String>();
	    Set<String> set2 =   new HashSet<String>();
	    
	    String[] parts = s1.split(" ");
	    for(String p : parts) {
	    	set1.add(p);
	    }
	    
	    String[] parts2 = s2.split(" ");
	    for(String p : parts2) {
	    	set2.add(p);
	    }
	 
	    
	   intersect.clear();
	   intersect.addAll(set1);
	   intersect.retainAll(set2);
	   union.clear();
	   union.addAll(set1);
	   union.addAll(set2);
	   System.out.println((double)intersect.size()/(double)union.size());

		
	}
	
}
