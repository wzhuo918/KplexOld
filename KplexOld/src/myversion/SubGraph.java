package myversion;
 
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

public class SubGraph {
	private HashMap<Integer,Integer> candidate;
	private HashMap<Integer,Integer> not;
	private HashMap<Integer,Integer> result;
	public SubGraph()
	{
		
	}
	public SubGraph(HashMap<Integer, Integer> tmpcand,
			HashMap<Integer, Integer> tmpres, HashMap<Integer, Integer> tmpnot) {
		this.candidate = tmpcand;
		this.not = tmpnot;
		this.result = tmpres;
	}

	public HashMap<Integer, Integer> getCandidate() {
		return candidate;
	}
	public void setCandidate(HashMap<Integer, Integer> candidate) {
		this.candidate = candidate;
	}
	public HashMap<Integer, Integer> getNot() {
		return not;
	}
	public void setNot(HashMap<Integer, Integer> nouse) {
		this.not = nouse;
	}
	public HashMap<Integer, Integer> getResult() {
		return result;
	}
	public void setResult(HashMap<Integer, Integer> result) {
		this.result = result;
	}
	public void readInString(String substring) {
		StringTokenizer st = new StringTokenizer(substring);
		int len  = Integer.valueOf(st.nextToken());
		this.candidate = new HashMap<Integer,Integer>(len);
		for(;len>0;len--){
			candidate.put(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
		}
		len  = Integer.valueOf(st.nextToken());
		this.not = new HashMap<Integer,Integer>(len);
		for(;len>0;len--){
			not.put(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
		}
		len  = Integer.valueOf(st.nextToken());
		this.result = new HashMap<Integer,Integer>(len);
		for(;len>0;len--){
			result.put(Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
		}
	}
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(candidate.size());
		for(Entry<Integer,Integer> en : candidate.entrySet()){
			sb.append(" ");
			sb.append(en.getKey());
			sb.append(" ");
			sb.append(en.getValue());
		}
		sb.append(" ");
		sb.append(not.size());
		for(Entry<Integer,Integer> en : not.entrySet()){
			sb.append(" ");
			sb.append(en.getKey());
			sb.append(" ");
			sb.append(en.getValue());
		}
		sb.append(" ");
		sb.append(result.size());
		for(Entry<Integer,Integer> en : result.entrySet()){
			sb.append(" ");
			sb.append(en.getKey());
			sb.append(" ");
			sb.append(en.getValue());
		}
		return sb.toString();
	}
}
