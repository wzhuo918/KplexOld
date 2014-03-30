package kplexold;


public class Pair implements Comparable<Pair>, Cloneable{
	public Integer point;
	public int rdeg;
	public int cdeg;
	/**
	 * @param point
	 * @param discon
	 * @param deg
	 */
	public Pair(int point, int discon) {
		super();
		this.point = point;
		this.rdeg = discon;
	}
	public Pair(int point, int discon,int cdeg) {
		super();
		this.point = point;
		this.rdeg = discon;
		this.cdeg = cdeg;
	}
	public Pair(){
		this.point = -1;
		this.cdeg = -1;
		this.rdeg = -1;
	}
	public Pair(Pair c) {
		this.point = c.point;
		this.cdeg = c.cdeg;
		this.rdeg = c.rdeg;
	}
	@Override
	public int compareTo(Pair p) {
		//return deg-((Pair)o).deg;//deg由小到大
		return (p.cdeg+p.rdeg)-(rdeg+cdeg);//deg由大到小
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return point.hashCode();
	}
	@Override
	public String toString(){
		//return "point:"+point+" rdeg,"+rdeg+" cdeg,"+cdeg;
		return ""+point;
	}
	/* 两个pair相等只看其对应的点是不是相等,每个点同时只能出现在一个集合中
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj==null||!(obj instanceof Pair))
			return false;
		return this.point==((Pair)obj).point;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	@Override
	public Pair clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return (Pair) super.clone();
	}

}