package search;
 
import java.util.HashMap;

public class SubGraph {
	private HashMap<Integer,Integer> candidate;
	private HashMap<Integer,Integer> nouse;
	private HashMap<Integer,Integer> result;
	public SubGraph(int sizea,int sizeb)
	{

	}
	public SubGraph(HashMap<Integer, Integer> tmpcand,
			HashMap<Integer, Integer> tmpres, HashMap<Integer, Integer> tmpnot) {
		this.candidate = tmpcand;
		this.nouse = tmpnot;
		this.result = tmpres;
	}

	public HashMap<Integer, Integer> getCandidate() {
		return candidate;
	}
	public void setCandidate(HashMap<Integer, Integer> candidate) {
		this.candidate = candidate;
	}
	public HashMap<Integer, Integer> getNouse() {
		return nouse;
	}
	public void setNouse(HashMap<Integer, Integer> nouse) {
		this.nouse = nouse;
	}
	public HashMap<Integer, Integer> getResult() {
		return result;
	}
	public void setResult(HashMap<Integer, Integer> result) {
		this.result = result;
	}
	
}
