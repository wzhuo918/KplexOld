package kplexold;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TreeMap;

import search.SubGraph;
import search.statusRecord;

public class LocalModeOldWithNot {

	// reduce的数目
	static int reduceNumber = 136;
	// 合法的kplex-clique大小，小于该值的kplex-clique被认为意义不大
	static int quasiCliqueSize = 5;
	// kplex-clique中k值
	static int k_plex = 2;
	//
	public static int kPlexSize = 0;
	//
	public static int numberKplex = 0;
	// “一跳”数据在内存中的形式
	public static HashMap<Integer, HashSet<Integer>> oneLeap = new HashMap<Integer, HashSet<Integer>>(
			7000);
	// 所有节点的集合，是ArrayList
	public static HashSet<Integer> nodeSet = new HashSet<Integer>(1000);
	public static ArrayList<statusRecord> stack = new ArrayList<statusRecord>(
			6000);
	public static ArrayList<Integer> result = new ArrayList<Integer>(30);
	public static int totalPart = 36;// 分散成为多少部分
	// 需要计算kplex的节点列表
	public static ArrayList<Integer> pick = new ArrayList<Integer>();
	// 使用hashset更新结果集，比ArrayList速度更快
	public static HashSet<Integer> hs = new HashSet<Integer>();
	public static PrintStream ps=null;
	public static boolean disconnect(int a, int b) {
		if (!(oneLeap.get(a)).contains(b))
			return true;
		else
			return false;
	}

	public static void getCriticalSet(HashMap<Integer,Integer> res,
			ArrayList<Integer> critical) {
		int numberDisconnect = 0;
		for (int vo : res.keySet()) {
			for (int vi : res.keySet()) {
				if (vo != vi && disconnect(vo, vi))
					numberDisconnect++;
			}
			// 有些节点已经是边界了，新加的节点必须和这些节点都相连
			if (numberDisconnect == k_plex - 1)
				critical.add(vo);
			res.put(vo, numberDisconnect);
			numberDisconnect = 0;// 复位
		}
	}

	/*
	 * public static void getTcurrent(long current, ArrayList<Long> aim) {
	 * ArrayList<Long> adj = edgeSet.get(current); for(Long v : adj)//邻节点
	 * aim.add(v); for(Long v : adj)//邻节点的邻节点 for(Long vi : edgeSet.get(v)) {
	 * if(!aim.contains(vi)) aim.add(vi); } }
	 */
	public static void updateCurCand(ArrayList<Integer> curCand,
			ArrayList<Integer> curNot, int v, ArrayList<Integer> res) {
		HashSet<Integer> vadj = oneLeap.get(v);
		for (int va : vadj)// 扩展待选集
		{
			if (!curNot.contains(va) && !res.contains(va)
					&& !curCand.contains(va))
				curCand.add(va);
		}
	}

	/**
	 * 删除掉v的邻接点中curCand,curNot,res的点 再将结果加到curCand中
	 * 
	 * @param curCand
	 * @param curNot
	 * @param v
	 * @param res
	 */
	public static void updateCurCand2(ArrayList<Integer> curCand,
			ArrayList<Integer> curNot, int v, ArrayList<Integer> res) {
		HashSet<Integer> vadj = oneLeap.get(v);
		hs.clear();
		hs.addAll(vadj);
		hs.removeAll(curCand);
		hs.removeAll(curNot);
		hs.removeAll(res);
		curCand.addAll(hs);
	}


static int cliquenum = 0;
static int dupnum = 0;
	public static void computeOneleapData(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		StringTokenizer stk;
		readInOneLeapData(file);
		// 将节点编号排序，为了使得各个reduce处理不同的部分从而并行化
		// 通过取余判定
		//Collections.sort(nodeSet);
		long t1 = System.currentTimeMillis();
		for (Integer current:nodeSet) {
			if(current==660)// 只处理这些节点
//			if (current%reduceNumber==0)
			//if(true)
			{
				// kPlexSize = 0;
				// kplex的数目
				numberKplex = 0;
				// 当前所求节点
				//int current = nodeSet.get(i);

				int sizeKplex = -1;
				stack.clear();
				HashMap<Integer,Integer> candidate = new HashMap<Integer,Integer>();
				HashMap<Integer,Integer> not = new HashMap<Integer,Integer>();
				// 状态栈，记录所有状态
				Stack<SubGraph> stack = new Stack<SubGraph>();
				// 每个k-plex结果集节点列表
				HashMap<Integer,Integer> resultKplex = new HashMap<Integer,Integer>();
				// 当前已经是(k-1)-plex，成为临界状态
				//ArrayList<Integer> critSet = new ArrayList<Integer>();
				// 当前节点是必须不能删除的,也就没有用了，不会再次加入
				//noUse.add(current);
				resultKplex.put(current, 0);
				HashSet<Integer> adj = oneLeap.get(current);
				HashSet<Integer> twoleap = new HashSet<Integer>();
				int curdeg = adj.size();
				int tmpdeg = 0;
				for(Integer node:adj){
					HashSet<Integer> nadj = oneLeap.get(node);
					twoleap.addAll(nadj);
					tmpdeg = nadj.size();
					if (tmpdeg > curdeg) {
						not.put(node,0);
					} else if (tmpdeg < curdeg) {
						candidate.put(node,0);
					} else {
						if (node > current) {
							candidate.put(node,0);
						} else if (node < current) {
							not.put(node,0);
						}
					}
				}
				twoleap.removeAll(adj);
				twoleap.remove(current);
				for(Integer node:twoleap){
					HashSet<Integer> nadj = oneLeap.get(node);
					tmpdeg = nadj.size();
					if (tmpdeg > curdeg) {
						not.put(node,1);
					} else if (tmpdeg < curdeg) {
						candidate.put(node,1);
					} else {
						if (node > current) {
							candidate.put(node,1);
						} else if (node < current) {
							not.put(node,1);
						}
					}
				}
				// 结果集合，初始时只有目标节点
				//resultKplex.put(current,0);
				// 当前备选节点为当前节点的邻节点
				//candidate.addAll(oneLeap.get(current));
				// 记录当前备选节点，无用节点
				SubGraph init = new SubGraph(candidate.size(),
						not.size());
				init.setCandidate(candidate);
				init.setNouse(not);
				init.setResult(resultKplex);
				// 初始状态
				stack.add(init);
				while (!stack.isEmpty())// 状态栈不空时执行操作
				{
					// 取栈顶元素,并读取状态赋给当前状态
					SubGraph top = stack.pop();
					HashMap<Integer,Integer> curCand = top.getCandidate();
					HashMap<Integer,Integer> curNot = top.getNouse();
					HashMap<Integer,Integer> curRes = top.getResult();
					HashSet<Integer> conncand = getConnSet(curCand,curRes);
					Iterator<Integer> iter = conncand.iterator();
					while(iter.hasNext()){
						Integer vp = iter.next();
						iter.remove();
						int counter = curCand.get(vp);
						curCand.remove(vp);

						if(curRes.size()+curCand.size()+1<quasiCliqueSize)
							continue;
						HashMap<Integer,Integer>tmpcand = (HashMap<Integer, Integer>) curCand.clone();
						HashMap<Integer,Integer>tmpnot = (HashMap<Integer, Integer>) curNot.clone();
						HashMap<Integer,Integer>tmpres = (HashMap<Integer, Integer>) curRes.clone();
						//tmpres.put(vp, -1);
						ArrayList<Integer> cirtset = updaterescount(tmpres,vp);
						HashSet<Integer> intersection = new HashSet<Integer>();
						// 若临界集合不为空，则新加入的节点只能属于临界集合邻节点的交集，否则会超过k-plex
						if (cirtset.size() > 0) {
							// 先加入第一个元素，再不断取交集
							intersection.addAll(oneLeap.get(cirtset.get(0)));
							for (int in = 1; in < cirtset.size(); in++) {
								HashSet<Integer> ad= oneLeap.get(cirtset
										.get(in));
								intersection.retainAll(ad);
							}
							intersection.removeAll(resultKplex.keySet());// 将已经存在结果中的节点删除
							tmpcand.keySet().retainAll(intersection);
							tmpnot.keySet().retainAll(intersection);
						}
						updatecount(tmpcand,vp);
						updatecount(tmpnot,vp);
						HashSet<Integer> cocand = getConnSet(tmpcand,tmpres);
						HashSet<Integer> conot = getConnSet(tmpnot,tmpres);
						
						if(cocand.isEmpty()){
							if(conot.isEmpty()&&tmpres.size()>=quasiCliqueSize){
								String re = tmpres.keySet().toString();
								System.out.println(re.substring(1, re.length()-1));
								cliquenum++;
							}else{
								dupnum++;
							}
							continue;
						}else{
							SubGraph sub = new SubGraph(tmpcand,tmpres,tmpnot);
							stack.add(sub);
						}
						curNot.put(vp, counter);
					}
				}
			}
		}
		long t2 = System.currentTimeMillis();// 统计计算该节点的k-plex的时间
		System.out.println("==========" + (t2 - t1) / 1000);
	}

	/**
	 * 更新可能节点(candidate或者not集)的计数(与result集中节点不相邻的个数)值
	 * 超出K_plex-1的点直接删除;
	 * 保持条件:candidate或者not中的任意一个点单独拿出来加到结果集里面都成为一个kplex
	 * @param curCand
	 * @param vp
	 */
	private static void updatecount(HashMap<Integer, Integer> curCand,
			Integer vp) {
		HashSet<Integer>adj = oneLeap.get(vp);int num = 0;
		Iterator<Map.Entry<Integer, Integer>> ite = curCand.entrySet().iterator();
		while( ite.hasNext()){
			Map.Entry<Integer, Integer> en = ite.next();
			if(!adj.contains(en.getKey())){
				num = en.getValue();
				if(num>=k_plex-1){
					ite.remove();
				}else{
					en.setValue(num+1);
				}
			}
		}
	}

	/**
	 * 将点vp加入到结果集中,更新结果集中点的计数值(与结果集本身不相邻的点的个数)
	 * 顺便返回当前临界点集合
	 * @param tmpres
	 * @param vp
	 * @return
	 */
	private static ArrayList<Integer> updaterescount(HashMap<Integer, Integer> tmpres,
			Integer vp) {
		HashSet<Integer> adj = oneLeap.get(vp);
		ArrayList<Integer> crit = new ArrayList<Integer>();
		int num = 0;
		for(Map.Entry<Integer, Integer> en:tmpres.entrySet()){
			if(!adj.contains(en.getKey())){
				en.setValue(en.getValue()+1);
				num++;
				if(en.getValue()==k_plex-1)
					crit.add(en.getKey());
			}
		}
		tmpres.put(vp, num);
		if(num==k_plex-1)
			crit.add(vp);
		return crit;
	}

	private static HashSet<Integer> getConnSet(HashMap<Integer, Integer> curCand,
			HashMap<Integer, Integer> curRes) {
		HashSet<Integer> all  = new HashSet<Integer>();
		HashSet<Integer> res = new HashSet<Integer>();
		for(Integer r:curRes.keySet()){
			all.addAll(oneLeap.get(r));
		}
		for(Integer c:curCand.keySet()){
			if(all.contains(c))
				res.add(c);
		}
		return res;
	}
	public static void init(String[] args) throws NumberFormatException, IOException{
//		FileReader fr = new FileReader(new File("/home/youli/CliqueHadoop/kplexnew_COMMON.txt"));
//		BufferedReader bfr = new BufferedReader(fr);
//		// 提取出所有的pick节点
//		String record = "";
//		pick.clear();
//		while ((record = bfr.readLine()) != null) {
//			String[] adjInfos = record.split(" ");
//			for (int i = 1; i < adjInfos.length; i++)
//				pick.add(Integer.valueOf(adjInfos[i]));
//		}
//		bfr.close();
		ps=new PrintStream(new File(args[1]));
		System.setOut(ps);
		reduceNumber = 132;
		quasiCliqueSize = 5;
		k_plex = 2;
	}
	public static void readInOneLeapData(String file) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		StringTokenizer stk;
		while ((line = reader.readLine()) != null) {
			stk = new StringTokenizer(line);
			int k = Integer.parseInt(stk.nextToken());
			HashSet<Integer> adj = new HashSet<Integer>();
			nodeSet.add(k);
			while (stk.hasMoreTokens()) {
				adj.add(Integer.parseInt(stk.nextToken()));
			}
			oneLeap.put(k, adj);
		}
	}
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		init(args);
		computeOneleapData(args[0]);
		//testKplex(args);
	}
	private static void testKplex(String[] args) throws IOException {
		k_plex = 2;
		readInOneLeapData(args[0]);
		Compare(args[1]);
	}
	public static void Compare(String str1) throws NumberFormatException, IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(str1));
		String line;
		HashMap<Integer,List<List<Integer>>> id2kplex = new HashMap<Integer,List<List<Integer>>>();
		int num = 0;
		while((line=reader.readLine())!=null){
			String[] splits = line.split(",");
			ArrayList<Integer> kplex = new ArrayList<Integer>();
			for(String split:splits)
				kplex.add(Integer.parseInt(split.trim()));
			Collections.sort(kplex);
			List<List<Integer>> listkplex = id2kplex.get(kplex.get(0));
			if(listkplex==null){
				listkplex = new ArrayList<List<Integer>>();
				id2kplex.put(kplex.get(0), listkplex);
			}
			listkplex.add( kplex);
		}
		int notnum = 0;
		for (List<List<Integer>> kplexes : id2kplex.values()) {
		
			for (int i = 0; i < kplexes.size(); i++) {
				if(!isKplex(kplexes.get(i))){
					System.out.println(kplexes.get(i)+" is not kplex");
					notnum++;
				}
				for (int j = 0; j < kplexes.size(); j++) {
					if (i!=j&&kplexes.get(j).containsAll(kplexes.get(i))){
						System.out.println(kplexes.get(j)+"r1,dup"+kplexes.get(i));
						num++;
					}
				}
			}
		}
		System.out.println("dup num"+num);
		System.out.println("not num"+notnum);
	}
		

	private static boolean isKplex(List<Integer> list) {
		for(Integer n:list){
			int num = 0;
			for(Integer i:list){
				if(n!=i&&disconnect(n,i))
					num++;
			}
			if(num>k_plex-1)
				return false;
		}
		return true;
	}
	public static void test(String file1, String file2) {
		ReadFile fi = new ReadFile();
		fi.readFileByLines(file1);
		Map<Integer, List<String>> k2c = fi.resultfile;
		List<List<Integer>> cliques = new ArrayList<List<Integer>>();
		for (List<String> ls : k2c.values()) {
			for (String s : ls) {
				ArrayList<Integer> tempdata = new ArrayList<Integer>();
				String[] str = s.split(" ");
				for (int i = 0; i < str.length; i++) {
					tempdata.add(Integer.parseInt(str[i]));
				}
				cliques.add(tempdata);
			}
		}
		/**
		 * for(int i = cliques.size()-1; i>=0;i--){ List<Integer> tl =
		 * cliques.get(i); for(int j = 0;j<i;j++){
		 * if(cliques.get(j).containsAll(tl)){
		 * System.out.println(cliques.get(j)+" contains all "+tl); // return; }
		 * } } System.out.println("test ok");
		 */
		ReadFile fj = new ReadFile();
		fj.readFileByLines(file2);
		Map<Integer, List<String>> k2c2 = fi.resultfile;
		List<List<Integer>> cliques2 = new ArrayList<List<Integer>>();
		for (List<String> ls : k2c2.values()) {
			for (String s : ls) {
				ArrayList<Integer> tempdata = new ArrayList<Integer>();
				String[] str = s.split(" ");
				for (int i = 0; i < str.length; i++) {
					tempdata.add(Integer.parseInt(str[i]));
				}
				cliques2.add(tempdata);
			}
		}
		if (cliques.size() != cliques2.size()) {
			System.out.println("size not equal");
			// return;
		}
		for (int i = cliques.size() - 1; i >= 0; i--) {
			if (!cliques2.contains(cliques.get(i))) {
				System.out.println("clique2 not contain" + cliques.get(i));
			}
			/**
			 * if(!(cliques.get(i).containsAll(cliques2.get(i))&&cliques2.get(i)
			 * .containsAll(cliques.get(i)))) {
			 * System.out.println("clique not equal"); return; }
			 */
		}
		System.out.println("compare ok");
	}


static	class ReadFile {
		public TreeMap<Integer, List<String>> resultfile = new TreeMap<Integer, List<String>>();

		public void readFileByLines(String fileName) {
			File file = new File(fileName);
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				int i = 0;
				String tempString = "";
				StringBuilder resultStr = new StringBuilder();
				while ((tempString = reader.readLine()) != null) {
					// resultStr = "";
					String[] strs = tempString.trim().split(",");
					
					ArrayList<Integer> tempdata = new ArrayList<Integer>();
					tempdata.add(Integer.parseInt(strs[0].trim()));
					String[] str = strs[1].split(" ");
					for (i = 0; i < str.length; i++) {
						str[i].trim();
						if (str[i].equals(""))
							continue;
						tempdata.add(Integer.parseInt(str[i]));
					}
					Collections.sort(tempdata);
					for (i = 0; i < tempdata.size(); i++) {
						resultStr.append(tempdata.get(i).toString() + " ");
					}
					List<String> cliques = resultfile.get(tempdata.get(0));
					if (cliques == null) {
						cliques = new ArrayList<String>();
						resultfile.put(tempdata.get(0), cliques);
					}
					cliques.add(resultStr.toString());
					resultStr.delete(0, resultStr.length());
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
					}
				}
			}
		}
	}

}
