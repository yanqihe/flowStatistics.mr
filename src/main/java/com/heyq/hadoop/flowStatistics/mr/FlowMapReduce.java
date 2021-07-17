package com.heyq.hadoop.flowStatistics.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowMapReduce {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "flow statistics");
		
		// 临时处理方法：会使输出结果都在part-r-00000，虽然满足了按电话号码有序输出，但集群环境中不能并发处理
		job.setNumReduceTasks(1);

		job.setJarByClass(FlowMapReduce.class);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean b = job.waitForCompletion(true);
		if (!b) {
			System.out.println("flow statistics task fail!");
		}
	}
}
