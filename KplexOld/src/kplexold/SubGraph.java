package kplexold;
 
import java.util.HashMap;

public class SubGraph {
	private HashMap<Integer,Integer> candidate;
	private HashMap<Integer,Pair> not;
	private HashMap<Integer,Integer> result;
	public SubGraph(int sizea,int sizeb)
	{

	}
	public SubGraph(HashMap<Integer, Integer> tmpcand,
			HashMap<Integer, Integer> tmpres, HashMap<Integer, Pair> tmpnot) {
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
	public HashMap<Integer, Pair> getNot() {
		return not;
	}
	public void setNot(HashMap<Integer, Pair> nouse) {
		this.not = nouse;
	}
	public HashMap<Integer, Integer> getResult() {
		return result;
	}
	public void setResult(HashMap<Integer, Integer> result) {
		this.result = result;
	}
	
}
