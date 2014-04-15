package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import myversion.SubGraph;

public class StaticCommonMethods {

	// reduce的数目
	public static int reduceNumber = 136;
	// 合法的kplex-clique大小，小于该值的kplex-clique被认为意义不大
	public static int quasiCliqueSize = 5;
	// kplex-clique中k值
	public static int k_plex = 2;
	// “一跳”数据在内存中的形式
	public static HashMap<Integer, HashSet<Integer>> oneLeap = new HashMap<Integer, HashSet<Integer>>(
			7000);
	// 所有节点的集合，是ArrayList
	public static HashSet<Integer> nodeSet = new HashSet<Integer>(1000);
	// 需要计算kplex的节点列表
	public static ArrayList<Integer> pick = new ArrayList<Integer>();
	// 使用hashset更新结果集，比ArrayList速度更快
	public static HashSet<Integer> hs = new HashSet<Integer>();
	public static PrintStream ps = null;
	public static Stack<SubGraph> stack = new Stack<SubGraph>();
	public static long time = 0;
	public static int cliquenum = 0;
	public static int dupnum = 0;
	public static int treesize = 0;
	public static String rootdir = "/home/"+RunOver.usr+"/QuasicClique/";
	public static String graphFile = "";
	public static int count = 0;
	public static long T = 0;
	public static int N = 0;
	public static FileWriter writer = null;
	public static int reduceid = 0;
	// 平均分配到各个计算节点之上,每个reduce保存一份“一跳”
	public static class oneLeapFinderPartitioner extends
			Partitioner<IntWritable, Text> {
		@Override
		public int getPartition(IntWritable key, Text value, int num) {
			return (int) ((key.get()) % num);
		}
	}

	public static SubGraph initSize1SubGraph(int current) {
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
			tmpdeg = oneLeap.get(node).size();
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
			return null;
		SubGraph init = new SubGraph();
		init.setCandidate(candidate);
		init.setNot(not);
		init.setResult(resultKplex);
		return init;
	}

	@SuppressWarnings("unchecked")
	public static long computeOneSubGraph(SubGraph top, boolean spillBig,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		// 将节点编号排序，为了使得各个reduce处理不同的部分从而并行化
		// 通过取余判定
		long t1 = System.currentTimeMillis();
		HashMap<Integer, Integer> curCand = top.getCandidate();
		HashMap<Integer, Integer> curNot = top.getNot();
		HashMap<Integer, Integer> curRes = top.getResult();
		HashMap<Integer, Integer> connnot = new HashMap<Integer, Integer>();
		HashSet<Integer> conncand = getConnSet(curCand, curRes, connnot, curNot);
		boolean isCritEmpty = isCritEmpth(curRes);
		HashMap<Integer, Integer> prunableNot = new HashMap<Integer, Integer>();
		if (!isCritEmpty) {
			try {
				prunableNot = getPrunableNot(connnot);
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			updateNotCdeg(prunableNot, curCand);
		}
		Iterator<Integer> iter = conncand.iterator();
		while (curRes.size() + curCand.size() >= quasiCliqueSize) {
			if (conncand.isEmpty()) {
				if (connnot.isEmpty()) {
					if (curRes.size() >= quasiCliqueSize) {
						String re = curRes.keySet().toString();
						context.write(new Text(re.substring(1, re.length() - 1)),NullWritable.get());
						cliquenum++;
					}
				} else {
					dupnum++;
				}
				break;
			}
			Integer vp = null;
			// 选择分裂点算法
			if (isCritEmpty || prunableNot.isEmpty()) {
				vp = iter.next();
				iter.remove();
			} else {
				int minpoint = 0, mindeg = curCand.size() + 10000;
				for (Entry<Integer, Integer> en : prunableNot.entrySet()) {
					if (en.getValue() < mindeg) {
						mindeg = en.getValue();
						minpoint = en.getKey();
					}
				}
				if (mindeg == 0)
					vp = -1;
				else {
					HashSet<Integer> set = oneLeap.get(minpoint);
					iter = conncand.iterator();
					while (iter.hasNext()) {
						Integer po = iter.next();
						if (!set.contains(po)) {
							vp = po;
							iter.remove();
							break;
						}
					}
				}
			}
			if (vp == -1)// 剪枝
				break;

			int counter = curCand.get(vp);
			curCand.remove(vp);
			// 从候选集中删除一个节点以后需要更新当前prunable not集中点的cdeg
			if (!isCritEmpty && !prunableNot.isEmpty()) {
				HashSet<Integer> set = oneLeap.get(vp);
				for (Entry<Integer, Integer> n : prunableNot.entrySet()) {
					if (!set.contains(n.getKey()))
						n.setValue(n.getValue() - 1);
				}
			}
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
					HashSet<Integer> ad = oneLeap.get(cirtset.get(in));
					intersection.retainAll(ad);
				}
				// intersection.removeAll(resultKplex.keySet());//
				// 将已经存在结果中的节点删除
				tmpcand.keySet().retainAll(intersection);
				tmpnot.keySet().retainAll(intersection);
			}
			updatecount(tmpcand, vp);
			updatecount(tmpnot, vp);
			if (tmpcand.size() + tmpres.size() >= quasiCliqueSize) {
				SubGraph sub = new SubGraph(tmpcand, tmpres, tmpnot);
				if(spillBig&&tmpcand.size()>N){
					spillToDisk(writer,sub);
				}else
				stack.add(sub);
				treesize++;
			}
			curNot.put(vp, counter);
			connnot.put(vp, counter);
			if (counter == 0) {
				prunableNot.put(vp, countNotAdj(curCand, vp));
			}
		}
		long t2 = System.currentTimeMillis();
		return t2 - t1;
	}

	public static int countNotAdj(HashMap<Integer, Integer> curCand, Integer vp) {
		int num = 0;
		HashSet<Integer> adj = oneLeap.get(vp);
		if (adj.size() < curCand.size()) {
			for (Integer a : adj) {
				if (curCand.containsKey(a))
					num++;
			}
		} else {
			for (Integer a : curCand.keySet()) {
				if (adj.contains(a))
					num++;
			}
		}
		return curCand.size() - num;
	}

	public static void spillToDisk(FileWriter writer, SubGraph pop)
			throws IOException {
		writer.write(((count++) % reduceNumber) + " " + pop.getResult().size()
				+ "%");
		writer.write(pop.toString());
		writer.write("\n");
	}

	/**
	 * 从not集中选出prunable集合(not中与所有res集所有点都相邻的节点) 选出的点是clone过来的,不影响原有curNot中的状态
	 * 
	 * @param connnot
	 * @return
	 * @throws CloneNotSupportedException
	 */
	public static HashMap<Integer, Integer> getPrunableNot(
			HashMap<Integer, Integer> connnot)
			throws CloneNotSupportedException {
		HashMap<Integer, Integer> re = new HashMap<Integer, Integer>();
		for (Entry<Integer, Integer> en : connnot.entrySet()) {
			if (en.getValue() == 0)
				re.put(en.getKey(), -1);
		}
		return re;
	}

	/**
	 * 判断临界点是否为空
	 * 
	 * @param curRes
	 * @return
	 */
	public static boolean isCritEmpth(HashMap<Integer, Integer> curRes) {
		for (Integer rdeg : curRes.values())
			if (rdeg == k_plex - 1)
				return false;
		return true;
	}

	/**
	 * 计算not集中的点和当前候选集中的点不相邻的度数
	 * 
	 * @param prunableNot
	 *            not集
	 * @param curCand
	 *            候选集
	 */
	public static void updateNotCdeg(HashMap<Integer, Integer> prunableNot,
			HashMap<Integer, Integer> curCand) {
		for (Entry<Integer, Integer> p : prunableNot.entrySet()) {
			p.setValue(countNotAdj(curCand, p.getKey()));
		}
	}

	/**
	 * 判断候选集curCand中的点是不是没有点与结果集相邻(只要与结果集中的任意一点相邻即为与结果集相邻)
	 * 
	 * @param curCand
	 *            待判断的候选点集,可能是candidate或者not集
	 * @param curRes
	 *            已有的结果集
	 * @return true if 候选集中没有与结果集总点相邻的点; false other
	 */
	public static boolean isConnSetEmpty(
			HashMap<Integer, ? extends Object> curCand,
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
	 * @param curCand
	 *            要筛选的点集合,可能是candidate或者not集
	 * @param vp
	 *            将要加入结果集中的点
	 */
	public static void updatecount(HashMap<Integer, Integer> curCand, Integer vp) {
		HashSet<Integer> adj = oneLeap.get(vp);
		int num = 0;
		Iterator<Map.Entry<Integer, Integer>> ite = curCand.entrySet()
				.iterator();
		while (ite.hasNext()) {
			Map.Entry<Integer, Integer> en = ite.next();
			if (!adj.contains(en.getKey())) {
				num = en.getValue();
				num++;
				if (num > k_plex - 1) {
					ite.remove();
				} else {
					en.setValue(num);
				}
			}// else 点的计数值不会增加,也就不会受影响
		}
	}

	/**
	 * 将点vp加入到结果集中,更新结果集中点的计数值(与结果集本身不相邻的点的个数) 由于操作需要遍历整个结果集,可顺便返回当前临界点集合
	 * 
	 * @param tmpres
	 *            输入已有结果集
	 * @param vp
	 *            将要加入到结果集的分裂点
	 * @return vp加入结果集后形成的新的临界点集合
	 */
	public static ArrayList<Integer> updaterescount(
			HashMap<Integer, Integer> tmpres, Integer vp) {
		HashSet<Integer> adj = oneLeap.get(vp);
		ArrayList<Integer> crit = new ArrayList<Integer>();
		int num = 0;
		for (Map.Entry<Integer, Integer> en : tmpres.entrySet()) {
			if (!adj.contains(en.getKey())) {
				en.setValue(en.getValue() + 1);
				num++;
				// 只返回加入vp后新增的临界点,
				if (en.getValue() == k_plex - 1)
					crit.add(en.getKey());
			}
			// 原来已有的临界点起不到过滤效果反而会增加计算临界点邻接表的时间
			// if (en.getValue() == k_plex - 1)
			// crit.add(en.getKey());
		}
		tmpres.put(vp, num);
		if (num == k_plex - 1)
			crit.add(vp);
		return crit;
	}

	/**
	 * 返回输入集合curCand中和结果集相邻的点集合,输入集合curCand中的点只要与curRes中的任意 一个点相邻即为与结果集相邻
	 * 
	 * @param curCand
	 *            输入的候选集
	 * @param curRes
	 *            已有的结果集
	 * @return 候选集中与结果集相邻的点集合
	 */
	public static HashSet<Integer> getConnSet(
			HashMap<Integer, Integer> curCand,
			HashMap<Integer, Integer> curRes,
			HashMap<Integer, Integer> connnot, HashMap<Integer, Integer> curNot) {
		HashSet<Integer> all = new HashSet<Integer>();
		HashSet<Integer> res = new HashSet<Integer>();
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
				if (curCand.containsKey(a))
					res.add(a);
			}
		}
		if (curNot.size() < all.size()) {
			for (Entry<Integer, Integer> n : curNot.entrySet()) {
				if (all.contains(n.getKey()))
					connnot.put(n.getKey(), n.getValue());
			}
		} else {
			for (Integer a : all) {
				if (curNot.containsKey(a))
					connnot.put(a, curNot.get(a));
			}
		}
		return res;
	}

	/**
	 * 读入一跳数据集,生成邻接表存储在oneLeap里
	 * 
	 * @param file
	 *            一跳数据集文件路径
	 * @throws IOException
	 */
	public static void readInOneLeapData(String file) throws IOException {
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

}
