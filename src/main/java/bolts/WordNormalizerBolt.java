package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizerBolt extends BaseBasicBolt {

	public void cleanup() {
	}

	/**
	 * 每次接收到元组时都会被调用一次，还会再发布若干个元组
	 * <p>
	 * 从单词文件接收到文本行，并标准化它。
	 * 文本行会全部转化成小写，并切分它，从中得到所有单词。
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
	}

	/**
	 * 声明bolt将只会发布一个名为“word”的域
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}