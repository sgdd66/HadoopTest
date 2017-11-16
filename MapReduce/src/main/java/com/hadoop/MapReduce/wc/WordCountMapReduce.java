package com.hadoop.MapReduce.wc;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



public class WordCountMapReduce {
	public static void main(String[] args) throws Exception {
		//创建配置对象
		Configuration conf=new Configuration();
		//创建job对象
		Job job =Job.getInstance(conf,"wordcount");
		//创建运行job的主类
		job.setJarByClass(WordCountMapReduce.class);
		//设置mapper类
		job.setMapperClass(WordCountMapper.class);
		//设置reducer类
		job.setReducerClass(WordCountReduce.class);
		//设置map输出的key，value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reducer输出的key，value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入输出的路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01.jkxy.com:8020/words"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop01.jkxy.com:8020/out06"));
		
		//提交job
		boolean b=job.waitForCompletion(true);
		if(!b) {
			System.err.println("task failed");
		}
	}
}
