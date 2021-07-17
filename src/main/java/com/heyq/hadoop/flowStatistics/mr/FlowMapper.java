package com.heyq.hadoop.flowStatistics.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 行内容：1363157993044 	18211575961	94-71-AC-CD-E6-18:CMCC-EASY	120.196.100.99	iface.qiyi.com	视频网站	15	12	1527	2106	200
		String line = value.toString();
		
		String[] origFields = line.split("\\t");
		
		String phone = origFields[1];
		long upFlow = Long.parseLong(origFields[8]);
		long downFlow = Long.parseLong(origFields[9]);
		
		context.write(new Text(phone), new FlowBean(upFlow, downFlow));
	}
}
