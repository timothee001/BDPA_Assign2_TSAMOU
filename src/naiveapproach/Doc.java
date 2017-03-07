package naiveapproach;

import java.util.TreeSet;


public class Doc implements Comparable<Doc>{

	long id;
	String content;
	
	public Doc(long id, String content){
		this.id=id;
		this.content=content;
	}
	
	public long GetId(){
		return this.id;
	}
	
	public String GetContent(){
		return this.content;
	}
	

	
	@Override
	public int compareTo(Doc o) {
		// TODO Auto-generated method stub
		 if (this.GetId() > o.GetId()) {
	            return 1;
	        } else {
	            return -1;
	        }
	}

}
