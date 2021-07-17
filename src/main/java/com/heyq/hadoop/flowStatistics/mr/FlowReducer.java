package com.heyq.hadoop.flowStatistics.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		
		long sumUpFlow = 0;
		long sumDownFlow = 0;
		
		for (FlowBean flowBean : values) {
			sumUpFlow += flowBean.getUpFlow();
			sumDownFlow += flowBean.getDownFlow();
		}
		
		context.write(key, new FlowBean(sumUpFlow, sumDownFlow));
		
	}
	
	
}
