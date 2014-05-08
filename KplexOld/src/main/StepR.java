package main;

import static main.StaticCommonMethods.N;
import static main.StaticCommonMethods.T;
import static main.StaticCommonMethods.cliquenum;
import static main.StaticCommonMethods.computeOneSubGraph;
import static main.StaticCommonMethods.count;
import static main.StaticCommonMethods.graphFile;
import static main.StaticCommonMethods.initSize1SubGraph;
import static main.StaticCommonMethods.k_plex;
import static main.StaticCommonMethods.nodeSet;
import static main.StaticCommonMethods.oneLeap;
import static main.StaticCommonMethods.pick;
import static main.StaticCommonMethods.quasiCliqueSize;
import static main.StaticCommonMethods.reduceNumber;
import static main.StaticCommonMethods.reduceid;
import static main.StaticCommonMethods.rootdir;
import static main.StaticCommonMethods.spillToDisk;
import static main.StaticCommonMethods.stack;
import static main.StaticCommonMethods.time;
import static main.StaticCommonMethods.treesize;
import static main.StaticCommonMethods.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.StringTokenizer;

import myversion.SubGraph;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class StepR {
	public static class StepRMapper extends
			Mapper<LongWritable, Text, PairTypeInt, Text> {
		PairTypeInt k = new PairTypeInt();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String str = value.toString();
			int p = Integer.parseInt(str.substring(0, str.indexOf(" ")));
			k.setA(p); 
			
			String v = str.substring(str.indexOf("%") + 1, str.length());
			k.setB(Integer.parseInt(str.substring(str.indexOf(" ")+1,str.indexOf("%"))));
			context.write(k, new Text(v));
		}
	}

	// 平均分配到各个计算节点之上,每个reduce保存一份“一跳”
	public static class oneLeapFinderPartitioner extends
			Partitioner<PairTypeInt, Text> {
		@Override
		public int getPartition(PairTypeInt key, Text value, int num) {
			return (key.getA()) % num;
		}
	}

	public static class StepRReducer extends
			Reducer<PairTypeInt, Text, Text, NullWritable> {

		@Override
		// 读入pick和split的值
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileReader fr = new FileReader(new File(rootdir
					+ "kplexnew_COMMON.txt"));
			BufferedReader bfr = new BufferedReader(fr);
			// 提取出所有的pick节点
			String record = "";
			pick.clear();
			while ((record = bfr.readLine()) != null) {
				String[] adjInfos = record.split(" ");
				for (int i = 1; i < adjInfos.length; i++)
					pick.add(Integer.valueOf(adjInfos[i]));
			}
			bfr.close();

			FileReader fr3 = new FileReader(new File(rootdir
					+ "kplexnew_PARAMETER.txt"));
			BufferedReader bfr3 = new BufferedReader(fr3);
			// 提取出所有的参数
			String record3 = bfr3.readLine();
			String[] adjInfos = record3.split(" ");
			reduceNumber = Integer.valueOf(adjInfos[0]);
			quasiCliqueSize = Integer.valueOf(adjInfos[1]);
			k_plex = Integer.valueOf(adjInfos[2]);
			T = Integer.valueOf(adjInfos[3]) * 1000L;
			N = Integer.valueOf(adjInfos[4]);

			graphFile = bfr3.readLine();
			bfr3.close();
			count = new Random().nextInt(reduceNumber);
			readInOneLeapData(graphFile);
		}

		public static void readInOneLeapData(String file) throws IOException {
			long t1 = System.currentTimeMillis();
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
			long t2 = System.currentTimeMillis();
			System.out.println("read edgetime "+(t2-t1));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if(writer!=null)
				writer.close();
			System.out.println("in cleanup");
			File prevfile = new File(RunOver.spillPath + reduceid);
			if (prevfile.exists() && prevfile.length() > 0) {
				if (time < T) {
					System.out.println(prevfile+" is not empty and time is not out ");
					File curFile = new File(RunOver.spillPath + reduceid + "#");
					BufferedReader reader = new BufferedReader(new FileReader(
							prevfile));
					FileWriter newWriter = new FileWriter(curFile);
					writer = newWriter;
					String line = "";
					stack.clear();
					while (time < T && (line = reader.readLine()) != null) {
						SubGraph graph = new SubGraph();
						graph.readInString(line.substring(
								line.indexOf("%") + 1, line.length()));
						stack.add(graph);
						while (!stack.isEmpty() && time < T) {
							time += computeOneSubGraph(stack.pop(), false,
									context);
						}
					}
					while (!stack.isEmpty()) {
						spillToDisk(newWriter, stack.pop());
					}
					while ((line = reader.readLine()) != null) {
						newWriter.write(line + "\n");
					}
					newWriter.close();
					if (curFile.exists() && curFile.length() == 0) {
						System.out.println(curFile+" is empty and is " +curFile.delete()+" deleted");
					}
					reader.close();
					System.out.println(prevfile+" is empty and is " +prevfile.delete()+" deleted");
				}else{
					System.out.println(prevfile+" is not empty but time is out ");
				}
			} else if (prevfile.exists()) {
				System.out.println(prevfile+"is empty and is "+prevfile.delete()+" deleted");
			} else {
				System.out.println(prevfile+ " does not exist");
			}
			System.out.println("kplex num=" + cliquenum + "========" + time
					/ 1000 + " s, treesize=" + treesize);
			super.cleanup(context);
		}

		@Override
		protected void reduce(PairTypeInt key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			if (writer == null) {
				reduceid = key.getA();
				writer = new FileWriter(RunOver.spillPath + reduceid);
			}
			for (Text t : values) {
				String sStr = t.toString();
				if (time < T) {
					SubGraph subGraph = new SubGraph();
					subGraph.readInString(sStr);
					stack.add(subGraph);
					treesize++;
					while (time < T && !stack.isEmpty()) {
						time += computeOneSubGraph(stack.pop(), true, context);
					}
					while(!stack.isEmpty()){
						spillToDisk(writer,stack.pop());
					}
				} else {
					writer.write(((count++)%reduceNumber)+" 0%"+sStr);
					writer.write("\n");
				}
			}
		}
	}

}
