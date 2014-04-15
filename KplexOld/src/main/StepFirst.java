package main;

import static main.StaticCommonMethods.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import myversion.SubGraph;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class StepFirst {

	public static class StepFirstMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileReader fr3 = new FileReader(new File(rootdir
					+ "kplexnew_PARAMETER.txt"));
			BufferedReader bfr3 = new BufferedReader(fr3);
			// 提取出所有的节点列表和节点以及邻节点的hash表
			String record3 = "";
			while ((record3 = bfr3.readLine()) != null) {
				String[] adjInfos = record3.split(" ");
				reduceNumber = Integer.valueOf(adjInfos[1]);
			}
			bfr3.close();
		}

		@Override
		// 若第一跳相同，则是聚起来的
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (int i = 0; i < reduceNumber; i++) {
				context.write(new IntWritable(i), value);
			}
		}
	}

	// 平均分配到各个计算节点之上,每个reduce保存一份“一跳”
	public static class oneLeapFinderPartitioner extends
			Partitioner<IntWritable, Text> {
		@Override
		public int getPartition(IntWritable key, Text value, int num) {
			return (key.get()) % num;
		}
	}

	public static class StepFirstReducer extends
			Reducer<IntWritable, Text, Text, NullWritable> {

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
			String record3 = "";
			// split.clear();
			while ((record3 = bfr3.readLine()) != null) {
				String[] adjInfos = record3.split(" ");
				graphFile = adjInfos[0];
				reduceNumber = Integer.valueOf(adjInfos[1]);
				quasiCliqueSize = Integer.valueOf(adjInfos[2]);
				k_plex = Integer.valueOf(adjInfos[3]);
				T = Integer.valueOf(adjInfos[4]) * 1000L;
				N = Integer.valueOf(adjInfos[5]);
			}
			bfr3.close();
			count = new Random().nextInt(reduceNumber);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			writer.close();
			File prevfile = new File(RunOver.spillPath + reduceid);
			if (prevfile.exists() && prevfile.length() > 0) {
				if (time < T) {
					File curFile = new File(RunOver.spillPath + reduceid + "#");
					BufferedReader reader = new BufferedReader(new FileReader(
							prevfile));
					FileWriter newWriter = new FileWriter(curFile);
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
							if (time >= T)
								break;
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
						curFile.delete();
					}
					reader.close();
					prevfile.delete();
				}
			} else if (prevfile.exists()) {
				prevfile.delete();
			}
			System.out.println("kplex num=" + cliquenum + "========" + time
					/ 1000 + " s, treesize=" + treesize);
			super.cleanup(context);
		}

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			nodeSet.clear();
			// reduce的编号，方便将所有节点分散到各个reduce进行计算
			int part = key.get();
			if (writer == null) {
				writer = new FileWriter(RunOver.spillPath + part);
				reduceid = part;
			}
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

			// 排序后，每个reduce只处理对应节点
			for (Integer current : nodeSet) {
				// if(current==19){
				if (current % reduceNumber == reduceid) {
//					if(pick.contains(current)){
					if(true){
						SubGraph init = initSize1SubGraph(current);
						if (init == null)
							continue;
						else if (time < T) {
							stack.add(init);
							treesize++;
							while (time < T && !stack.isEmpty()) {
								SubGraph top = stack.pop();
								time += computeOneSubGraph(top, true, context);
							}
							while (!stack.isEmpty()) {
								spillToDisk(writer, stack.pop());
							}
						} else {
							spillToDisk(writer, init);
						}
					}
					
				}
			}
		}
	}
}
