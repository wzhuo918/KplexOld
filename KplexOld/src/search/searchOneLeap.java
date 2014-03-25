package search;

//计算包含每个节点的最大k-plex quasiclique
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class searchOneLeap {

	//reduce的数目
	static int reduceNumber = 36;
	//合法的kplex-clique大小，小于该值的kplex-clique被认为意义不大
	static int quasiCliqueSize = 5;
	//kplex-clique中k值
	static int k_plex = 3;
	//
	public static int kPlexSize = 0;
	//
	public static int numberKplex = 0;
	// “一跳”数据在内存中的形式
	public static HashMap<Integer, HashSet<Integer>> oneLeap = new HashMap<Integer, HashSet<Integer>>(
			7000);
	//所有节点的集合，是ArrayList
	public static ArrayList<Integer> nodeSet = new ArrayList<Integer>(1000);
	public static ArrayList<statusRecord> stack = new ArrayList<statusRecord>(
			6000);
	public static ArrayList<Integer> result = new ArrayList<Integer>(30);
	public static int totalPart = 36;// 分散成为多少部分
	//需要计算kplex的节点列表
	public static ArrayList<Integer> pick = new ArrayList<Integer>();
	//使用hashset更新结果集，比ArrayList速度更快
	public static HashSet<Integer> hs = new HashSet<Integer>();
	//记录每个节点耗时的问流，方便实验
	public static PrintStream ps=null;
	//---------------------------------------------------------------------
	// 将“一跳”的数据每个reduce“备份”一份，数据量较小的时候没有问题，数据量较大时候
	//网络传输数据耗时较多
	//如果一开始时，每个节点拥有一份数据，则不需要通过mapper传递“一跳”的数据，可以每个机器
	//存一份，通过setup将该份数据读入内存，可以使得任务在mapper中完成，可少一次shuffle
	public static class oneLeapFinderMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileReader fr2 = new FileReader(new File("/home/dic/kplexold_PARAMETER.txt"));
			BufferedReader bfr2 = new BufferedReader(fr2);
			String record2 = "";
			//将需要计算kplex的节点保存在链表pick中
			pick.clear();			
			while ((record2 = bfr2.readLine()) != null) {
				String[] adjInfos = record2.split(" ");
				reduceNumber = Integer.valueOf(adjInfos[0]);
				quasiCliqueSize = Integer.valueOf(adjInfos[1]);
				k_plex = Integer.valueOf(adjInfos[2]);
				
			}
			bfr2.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (int i = 0; i < reduceNumber; i++) {
				context.write(new IntWritable(i), value);
			}

		}
	}
	
	//将mapper中生成的reduceNumber份数据发到每一个reduce中
	public static class oneLeapFinderPartitioner extends
			Partitioner<IntWritable, Text> {
		@Override
		public int getPartition(IntWritable key, Text value, int num) {
			return (int) ((key.get()) % num);// 平均分配到各个计算节点之上,每个计算节点保存一份“一跳”
		}
	}

	public static boolean disconnect(int a, int b) {
		if (!(oneLeap.get(a)).contains(b))
			return true;
		else
			return false;
	}

	public static void getCriticalSet(ArrayList<Integer> res,
			ArrayList<Integer> critical) {
		int numberDisconnect = 0;
		for (int vo : res) {
			for (int vi : res) {
				if (vo != vi && disconnect(vo, vi))
					numberDisconnect++;
			}
			// 有些节点已经是边界了，新加的节点必须和这些节点都相连
			if (numberDisconnect == k_plex - 1)
				critical.add(vo);
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
	 * 删除掉v的邻接点中curCand,curNot,res的点
	 * 再将结果加到curCand中
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
	public static void check_Kplex(ArrayList<Integer> curCand,
			ArrayList<Integer> res) {// 将在res中和自己之外仍有多余k_plex-1个节点不相连的节点删除，下一步不在需要
		int number = 0;
		ArrayList<Integer> critical = new ArrayList<Integer>();
		ArrayList<Integer> remove = new ArrayList<Integer>();
		getCriticalSet(res, critical);
		for (int cur : curCand) {
			number = 0;
			for (int cri : critical) {
				if (disconnect(cur, cri)) {
					remove.add(cur);
					break;
				}
			}
			for (int vres : res) {
				if (disconnect(vres, cur))
					number++;
			}
			if (number > k_plex - 1)
				remove.add(cur);
		}
		curCand.removeAll(remove);
	}

	//计算kplex的具体过程在reduce中完成
	public static class OneLeapFinderReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			//计算所有节点的kplex过于耗时，选择部分节点展开计算，具体计算哪些节点的kplex
			//记录再PAKDD_AIM.txt文件中
			FileReader fr = new FileReader(new File("/home/dic/kplexold_COMMON.txt"));
			BufferedReader bfr = new BufferedReader(fr);
			String record = "";
			//将需要计算kplex的节点保存在链表pick中
			pick.clear();			
			while ((record = bfr.readLine()) != null) {
				String[] adjInfos = record.split(" ");
				for (int i = 0; i < adjInfos.length; i++)
					pick.add(Integer.valueOf(adjInfos[i]));
			}
			bfr.close();
			
			FileReader fr2 = new FileReader(new File("/home/dic/kplexold_PARAMETER.txt"));
			BufferedReader bfr2 = new BufferedReader(fr2);
			String record2 = "";
			//将需要计算kplex的节点保存在链表pick中
			pick.clear();			
			while ((record2 = bfr2.readLine()) != null) {
				String[] adjInfos = record2.split(" ");
				reduceNumber = Integer.valueOf(adjInfos[0]);
				quasiCliqueSize = Integer.valueOf(adjInfos[1]);
				k_plex = Integer.valueOf(adjInfos[2]);
				
			}
			bfr2.close();
			
			//测试时候使用，从这一部分节点中选取耗时可以接受的部分节点
			//避免有些节点过大计算时长难以接受，为方便实验而进行记录，可用log4j记录
			//避免在日志文件中查找，保存在各个节点的指定文件中
			ps=new PrintStream(new File("/home/dic/timelook.txt"));
			System.setOut(ps);
		}

		//关闭setup中打开的流和收尾工作
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			ps.flush();
			ps.close();
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			//初始化
			nodeSet.clear();
			//当前reduce的编号，根据该编号选择需要计算的
			int part = key.get();
			for (Text t : values)// 获得一跳信息
			{
				String val = t.toString();
				String[] oneleap = val.split(" ");
				int node = Integer.valueOf(oneleap[0]);
				nodeSet.add(node);
				HashSet<Integer> adj = new HashSet<Integer>(80);
				for (int i = 1; i < oneleap.length; i++) {
					adj.add(Integer.valueOf(oneleap[i]));
				}
				oneLeap.put(node, adj);
			}
			//将节点编号排序，为了使得各个reduce处理不同的部分从而并行化
			//通过取余判定
			Collections.sort(nodeSet);
			for (int i = 0; i < nodeSet.size(); i++) {
				if (i % reduceNumber == part)// 只处理这些节点
				{
					//kPlexSize = 0;
					//kplex的数目
					numberKplex = 0;
					// 当前所求节点
					int current = nodeSet.get(i);
					if (pick.contains(current)) {
						long t1 = System.currentTimeMillis();
						int sizeKplex = -1;
						stack.clear();
						ArrayList<Integer> candidate = new ArrayList<Integer>();
						ArrayList<Integer> noUse = new ArrayList<Integer>();						
						//状态栈，记录所有状态
						ArrayList<statusRecord> stack = new ArrayList<statusRecord>();
						//每个k-plex结果集节点列表
						ArrayList<Integer> resultKplex = new ArrayList<Integer>();						
						// 当前已经是(k-1)-plex，成为临界状态
						ArrayList<Integer> critSet = new ArrayList<Integer>();
						// 当前节点是必须不能删除的,也就没有用了，不会再次加入
						noUse.add(current);
						//结果集合，初始时只有目标节点
						resultKplex.add(current);
						// 当前备选节点为当前节点的邻节点
						candidate.addAll(oneLeap.get(current));
						//记录当前备选节点，无用节点
						statusRecord init = new statusRecord(candidate.size(),
								noUse.size());
						init.setCandidate(candidate);
						init.setNouse(noUse);
						//初始状态
						stack.add(init);
						while (!stack.isEmpty())//状态栈不空时执行操作
						{
							// 取栈顶元素,并读取状态赋给当前状态
							statusRecord top = stack.get(stack.size() - 1);
							ArrayList<Integer> curCand = new ArrayList<Integer>();
							ArrayList<Integer> curNot = new ArrayList<Integer>();
							curCand.addAll(top.getCandidate());
							curNot.addAll(top.getNouse());
							int curPos = top.getPos();
							// 栈顶元素pos位置后移
							top.setPos((curPos + 1));
							//等于0时为初始状态，没有节点加入无用节点列表,每次将前一个元素加入无用列表不再考虑
							if (curPos > 0) {
								int vn = curCand.get(curPos - 1);
								curNot.add(vn);
								top.getNouse().add(vn);
							}
							//备选节点列表中仍有元素时候执行
							if (curPos <= curCand.size() - 1) {
								//当前节点
								int v = curCand.get(curPos);
								critSet.clear();
								//获取临界集合
								getCriticalSet(resultKplex, critSet);
								HashSet<Integer> intersection = new HashSet<Integer>();
								//若临界集合不为空，则新加入的节点只能属于临界集合邻节点的交集，否则会超过k-plex
								if (critSet.size() > 0)
								{
									//先加入第一个元素，再不断取交集
									intersection.addAll(oneLeap.get(critSet
											.get(0)));
									for (int in = 1; in < critSet.size(); in++) {
										HashSet<Integer> adj = oneLeap
												.get(critSet.get(in));
										intersection.retainAll(adj);
									}
									intersection.removeAll(resultKplex);// 将已经存在结果中的节点删除
								}
								//更新当前待选集合，新加入的节点v的邻接点过滤备选集，无用集，结果集
								//再并入原备选集，构成新的备选集
								//(adj<v>-curNot-result)||curCand
								updateCurCand2(curCand, curNot, v, resultKplex);
								curCand.removeAll(curNot);
								//和临界集合就交集
								if (!intersection.isEmpty())
									curCand.retainAll(intersection);
								//求交集之后当前节点仍然符合要求则入结果集
								if (curCand.contains(v)) {
									resultKplex.add(v);
									int p = curCand.indexOf(v);
									if (p != -1)
										curCand.remove(p);
									curNot.add(v);
								}
								//入栈前再判断一次k-plex特性，并过滤
								check_Kplex(curCand, resultKplex);
								//无备选节点，遍历结束判断并输出
								if (curCand.isEmpty()) {
									int tmpSizeKplex = resultKplex.size();
									numberKplex++;
									//sizeKplex是减少输出而已，总输出更大的kplex ，否则过多
									if (tmpSizeKplex >= sizeKplex
											&& tmpSizeKplex >= quasiCliqueSize)// 节点数目最小为5
									{
										context.write(
												new IntWritable(0),
												new Text(resultKplex.toString()));
										sizeKplex = tmpSizeKplex;
									}
									//最后入结果栈的元素删除，回溯
									resultKplex.remove(resultKplex.size() - 1);
								} else {//新状态入栈
									statusRecord sr = new statusRecord(
											curCand.size(), curNot.size());
									sr.setCandidate(curCand);
									sr.setNouse(curNot);
									stack.add(sr);// 新加入一层状态
								}
							} else {
								try {
									//备选集遍历完，回溯
									resultKplex.remove(resultKplex.size() - 1);
								} catch (ArrayIndexOutOfBoundsException e) {
									System.out.println(resultKplex.toString());
								}
								//备选状态的备选集遍历完，删除该备选状态
								stack.remove(top);
							}
						}
						long t2=System.currentTimeMillis();//统计计算该节点的k-plex的时间
						ps.println(current+"=========="+(t2-t1));
					}
				}
			}
		}
	}

}
