package kplexold;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import search.searchOneLeap;
public class countK_Plex {

	public static void main(String[] args) throws Exception
	{
		String in=args[0];
		String pre=args[1];
		int reducenum=Integer.valueOf(args[2]);
		Configuration conf = new Configuration();
		
		Job job = new Job(conf,"count kplex");
		job.setNumReduceTasks(reducenum);
		job.setJarByClass(countK_Plex.class);
		job.setMapperClass(searchOneLeap.oneLeapFinderMapper.class);
		job.setPartitionerClass(searchOneLeap.oneLeapFinderPartitioner.class);
		job.setReducerClass(searchOneLeap.OneLeapFinderReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(job, new Path(pre + "5"));
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(pre + "_kplexold"));
		
		
		long t1 = System.currentTimeMillis();		
		job.waitForCompletion(true);
		long t2 = System.currentTimeMillis();
					
		System.out.println("computer kplex:"+(t2-t1));

		 
	}
}
