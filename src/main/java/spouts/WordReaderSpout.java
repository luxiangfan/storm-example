package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReaderSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private FileReader fileReader;

	private boolean completed = false;

	public void close() {
	}

	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	/**
	 * 这个方法首先被调用, 将创建一个文件并维持一个collector对象
	 *
	 * @param conf      配置
	 * @param context   包含所有拓扑数据
	 * @param collector 它能让我们发布交给bolts处理的数据
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
		}

		this.collector = collector;
	}

	/**
	 * 这个方法做的惟一一件事情就是分发文件中的文本行
	 * 读取文件并逐行发布数据
	 * <p>
	 * nextTuple()会在同一个循环内被ack()和fail()周期性的调用。
	 * 没有任务时它必须释放对线程的控制，其它方法才有机会得以执行。
	 * 因此nextTuple的第一行就要检查是否已处理完成。
	 * 如果完成了，为了降低处理器负载，会在返回前休眠一秒。
	 * 如果任务完成了，文件中的每一行都已被读出并分发了。
	 */
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}

		String line;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((line = reader.readLine()) != null) {
				/** 按行发布一个新值 */
				this.collector.emit(new Values(line), line);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	/**
	 * 声明输入域"word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
