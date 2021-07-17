package com.heyq.hadoop.flowStatistics.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable {
	// 上传流量
	private long upFlow;

	// 下载流量
	private long downFlow;

	// 流量总和
	private long sumFlow;

	// 必须要有，反序列化要调用空参构造器
	public FlowBean() {
	}

	public FlowBean(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	/**
	 * 序列化
	 *
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	/**
	 * 反序列化 注：字段属性顺序必须一致
	 *
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
	
	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}