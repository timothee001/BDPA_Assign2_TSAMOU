package preprocessing;

import java.util.TreeSet;


public class WordDocs implements Comparable<WordDocs>{

	TreeSet<Long> tree;
	String key;
	
	public WordDocs(String key,TreeSet<Long> tree){
		this.key=key;
		this.tree=tree;
	}
	
	public String GetKey(){
		return this.key;
	}
	
	public int Size(){
		return this.tree.size();
	}
	
	public TreeSet<Long> GetTree(){
		return this.tree;
	}
	
	@Override
	public int compareTo(WordDocs o) {
		// TODO Auto-generated method stub
		 if (this.Size() > o.Size()) {
	            return 1;
	        } else {
	            return -1;
	        }
	}

}
