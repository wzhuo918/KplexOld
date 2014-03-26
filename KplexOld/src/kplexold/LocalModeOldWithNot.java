package kplexold;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;

import search.SubGraph;

public class LocalModeOldWithNot {

	// reduce的数目
	static int reduceNumber = 136;
	// 合法的kplex-clique大小，小于该值的kplex-clique被认为意义不大
	static int quasiCliqueSize = 5;
	// kplex-clique中k值
	static int k_plex = 2;
	// “一跳”数据在内存中的形式
	static HashMap<Integer, HashSet<Integer>> oneLeap = new HashMap<Integer, HashSet<Integer>>(
			7000);
	// 所有节点的集合，是ArrayList
	static HashSet<Integer> nodeSet = new HashSet<Integer>(1000);
	// 需要计算kplex的节点列表
	static ArrayList<Integer> pick = new ArrayList<Integer>();
	// 使用hashset更新结果集，比ArrayList速度更快
	static HashSet<Integer> hs = new HashSet<Integer>();
	static PrintStream ps = null;

	static int cliquenum = 0;
	static int dupnum = 0;
	static int treesize = 0;

	@SuppressWarnings("unchecked")
	private static void computeOneleapData(String file) throws IOException {
		readInOneLeapData(file);
		// 将节点编号排序，为了使得各个reduce处理不同的部分从而并行化
		// 通过取余判定
		long t1 = System.currentTimeMillis();
		for (Integer current : nodeSet) {
			// if(current==660)// 只处理这些节点
			if (current % reduceNumber == 0)
			// if(true)
			{
				HashMap<Integer, Integer> candidate = new HashMap<Integer, Integer>();
				HashMap<Integer, Integer> not = new HashMap<Integer, Integer>();
				// 状态栈，记录所有状态
				Stack<SubGraph> stack = new Stack<SubGraph>();
				// 每个k-plex结果集节点列表
				HashMap<Integer, Integer> resultKplex = new HashMap<Integer, Integer>();
				resultKplex.put(current, 0);
				HashSet<Integer> adj = oneLeap.get(current);
				HashSet<Integer> twoleap = new HashSet<Integer>();
				int curdeg = adj.size();
				int tmpdeg = 0;
				for (Integer node : adj) {
					HashSet<Integer> nadj = oneLeap.get(node);
					twoleap.addAll(nadj);
					tmpdeg = nadj.size();
					if (tmpdeg > curdeg) {
						not.put(node, 0);
					} else if (tmpdeg < curdeg) {
						candidate.put(node, 0);
					} else {
						if (node > current) {
							candidate.put(node, 0);
						} else if (node < current) {
							not.put(node, 0);
						}
					}
				}
				twoleap.removeAll(adj);
				twoleap.remove(current);
				for (Integer node : twoleap) {
					HashSet<Integer> nadj = oneLeap.get(node);
					tmpdeg = nadj.size();
					if (tmpdeg > curdeg) {
						not.put(node, 1);
					} else if (tmpdeg < curdeg) {
						candidate.put(node, 1);
					} else {
						if (node > current) {
							candidate.put(node, 1);
						} else if (node < current) {
							not.put(node, 1);
						}
					}
				}
				if (candidate.size() + 1 < quasiCliqueSize)
					continue;
				SubGraph init = new SubGraph(candidate.size(), not.size());
				init.setCandidate(candidate);
				init.setNouse(not);
				init.setResult(resultKplex);
				// 初始状态
				stack.add(init);
				treesize++;
				while (!stack.isEmpty())// 状态栈不空时执行操作
				{
					// 取栈顶元素,并读取状态赋给当前状态
					SubGraph top = stack.pop();
					HashMap<Integer, Integer> curCand = top.getCandidate();
					HashMap<Integer, Integer> curNot = top.getNouse();
					HashMap<Integer, Integer> curRes = top.getResult();
					ArrayList<Integer> conncand = getConnSet(curCand, curRes);
					Iterator<Integer> iter = conncand.iterator();
					while (iter.hasNext()
							&& curRes.size() + curCand.size() >= quasiCliqueSize) {
						Integer vp = iter.next();
						// iter.remove();
						int counter = curCand.get(vp);
						curCand.remove(vp);

						HashMap<Integer, Integer> tmpcand = (HashMap<Integer, Integer>) curCand
								.clone();
						HashMap<Integer, Integer> tmpnot = (HashMap<Integer, Integer>) curNot
								.clone();
						HashMap<Integer, Integer> tmpres = (HashMap<Integer, Integer>) curRes
								.clone();
						// 将点vp加入到结果集中,更新结果集中点的计数值(与结果集本身不相邻的点的个数) 顺便返回当前临界点集合
						ArrayList<Integer> cirtset = updaterescount(tmpres, vp);
						HashSet<Integer> intersection = new HashSet<Integer>();
						// 若临界集合不为空，则新加入的节点只能属于临界集合邻节点的交集，否则会超过k-plex
						if (cirtset.size() > 0) {
							// 先加入第一个元素，再不断取交集
							intersection.addAll(oneLeap.get(cirtset.get(0)));
							for (int in = 1; in < cirtset.size(); in++) {
								HashSet<Integer> ad = oneLeap.get(cirtset
										.get(in));
								intersection.retainAll(ad);
							}
							// intersection.removeAll(resultKplex.keySet());//
							// 将已经存在结果中的节点删除
							tmpcand.keySet().retainAll(intersection);
							tmpnot.keySet().retainAll(intersection);
						}
						updatecount(tmpcand, vp);
						updatecount(tmpnot, vp);
						boolean cocand = isConnSetEmpty(tmpcand, tmpres);
						boolean conot = isConnSetEmpty(tmpnot, tmpres);

						if (cocand) {
							if (conot && tmpres.size() >= quasiCliqueSize) {
								// String re = tmpres.keySet().toString();
								// System.out.println(re.substring(1,
								// re.length()-1));
								cliquenum++;
							} else {
								dupnum++;
							}
							continue;
						} else {
							if (tmpcand.size() + tmpres.size() >= quasiCliqueSize) {
								SubGraph sub = new SubGraph(tmpcand, tmpres,
										tmpnot);
								stack.add(sub);
								treesize++;
							}
						}
						curNot.put(vp, counter);
					}
				}
			}
		}
		long t2 = System.currentTimeMillis();// 统计计算该节点的k-plex的时间
		System.out.println("kplex num=" + cliquenum + "==========" + (t2 - t1)
				/ 1000 + " s, tree size=" + treesize);
	}

	/**
	 * 判断候选集curCand中的点是不是没有点与结果集相邻(只要与结果集中的任意一点相邻即为与结果集相邻)
	 * @param curCand 待判断的候选点集,可能是candidate或者not集
	 * @param curRes 已有的结果集
	 * @return true if 候选集中没有与结果集总点相邻的点; false other
	 */
	private static boolean isConnSetEmpty(HashMap<Integer, Integer> curCand,
			HashMap<Integer, Integer> curRes) {
		HashSet<Integer> all = new HashSet<Integer>();
		for (Integer r : curRes.keySet()) {
			all.addAll(oneLeap.get(r));
		}
		if (curCand.size() < all.size()) {
			for (Integer c : curCand.keySet()) {
				if (all.contains(c))
					return false;
			}
		} else {
			for (Integer a : all) {
				if (curCand.containsKey(a))
					return false;
			}
		}
		return true;
	}

	/**
	 * 更新可能节点(candidate或者not集)的计数(与result集中节点不相邻的个数)值 超出K_plex-1的点直接删除;
	 * 保持条件:candidate或者not中的任意一个点单独拿出来加到结果集里面都成为一个kplex
	 * 
	 * @param curCand 要筛选的点集合,可能是candidate或者not集
	 * @param vp 将要加入结果集中的点
	 */
	private static void updatecount(HashMap<Integer, Integer> curCand,
			Integer vp) {
		HashSet<Integer> adj = oneLeap.get(vp);
		int num = 0;
		Iterator<Map.Entry<Integer, Integer>> ite = curCand.entrySet()
				.iterator();
		while (ite.hasNext()) {
			Map.Entry<Integer, Integer> en = ite.next();
			if (!adj.contains(en.getKey())) {
				num = en.getValue();
				if (num >= k_plex - 1) {
					ite.remove();
				} else {
					en.setValue(num + 1);
				}
			}//else 点的计数值不会增加,也就不会受影响
		}
	}

	/**
	 * 将点vp加入到结果集中,更新结果集中点的计数值(与结果集本身不相邻的点的个数) 
	 * 由于操作需要遍历整个结果集,可顺便返回当前临界点集合
	 * @param tmpres 输入已有结果集
	 * @param vp 将要加入到结果集的分裂点
	 * @return vp加入结果集后形成的新的临界点集合
	 */
	private static ArrayList<Integer> updaterescount(
			HashMap<Integer, Integer> tmpres, Integer vp) {
		HashSet<Integer> adj = oneLeap.get(vp);
		ArrayList<Integer> crit = new ArrayList<Integer>();
		int num = 0;
		for (Map.Entry<Integer, Integer> en : tmpres.entrySet()) {
			if (!adj.contains(en.getKey())) {
				en.setValue(en.getValue() + 1);
				num++;
				//只返回加入vp后新增的临界点,
				if (en.getValue() == k_plex - 1)
					crit.add(en.getKey());
			}
			//原来已有的临界点起不到过滤效果反而会增加计算临界点邻接表的时间
//			if (en.getValue() == k_plex - 1)
//				crit.add(en.getKey());
		}
		tmpres.put(vp, num);
		if (num == k_plex - 1)
			crit.add(vp);
		return crit;
	}

	/**
	 * 返回输入集合curCand中和结果集相邻的点集合,输入集合curCand中的点只要与curRes中的任意
	 * 一个点相邻即为与结果集相邻
	 * @param curCand 输入的候选集
	 * @param curRes 已有的结果集
	 * @return 候选集中与结果集相邻的点集合
	 */
	private static ArrayList<Integer> getConnSet(
			HashMap<Integer, Integer> curCand, HashMap<Integer, Integer> curRes) {
		HashSet<Integer> all = new HashSet<Integer>();
		ArrayList<Integer> res = new ArrayList<Integer>();
		for (Integer r : curRes.keySet()) {
			all.addAll(oneLeap.get(r));
		}
		if (curCand.size() < all.size()) {
			for (Integer c : curCand.keySet()) {
				if (all.contains(c))
					res.add(c);
			}
		} else {
			for (Integer a : all) {
				if(curCand.containsKey(a))
					res.add(a);
			}
		}
		return res;
	}

	/**
	 * 初始化配置信息,包括取计算点的间隔reduceNumber,
	 * 有意义的最小kplex大小quasiCliqueSize,
	 * kplex 的k值
	 * @param args
	 */
	private static void init(String[] args){
		// ps=new PrintStream(new File(args[1]));
		// System.setOut(ps);
		reduceNumber = 132;
		quasiCliqueSize = 5;
		k_plex = 2;
	}

	/**
	 * 读入一跳数据集,生成邻接表存储在oneLeap里
	 * @param file 一跳数据集文件路径
	 * @throws IOException
	 */
	private static void readInOneLeapData(String file) throws IOException {
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
		reader.close();
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		init(args);
		computeOneleapData(args[0]);
		// testKplex(args);
	}
}
