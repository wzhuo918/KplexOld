package search;

import java.util.ArrayList;
import java.util.HashMap;

public class statusRecord {
	//记录该层状态涉及的节点
	private ArrayList<Integer> candidate;
	//不再使用的节点
	private ArrayList<Integer> nouse;
	//层数
	private int pos;
	public statusRecord(int sizea,int sizeb)
	{
		candidate = new ArrayList<Integer>(sizea);
		nouse= new ArrayList<Integer>(sizeb);
		pos=0;
		candidate.clear();
		nouse.clear();
	}
	public void setPos(int l)
	{
		this.pos=l;
	}
	public void setCandidate(ArrayList<Integer> init)
	{
		this.candidate.addAll(init);
	}
	public void setNouse(ArrayList<Integer> init)
	{
		this.nouse.addAll(init);
	}
	public ArrayList<Integer> getCandidate()
	{
		return this.candidate;
	}
	public ArrayList<Integer> getNouse()
	{
		return this.nouse;
	}
	public int getPos()
	{
		return this.pos;
	}
	
}
