import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounterBolt;
import bolts.WordNormalizerBolt;
import spouts.WordReaderSpout;


public class MyTopology {

	private static final String INPUT_FILE_PATH = "src/main/resources/words.txt";

	public static void main(String[] args) throws InterruptedException {
		// 定义拓扑
		/** 在spout和bolts之间通过shuffleGrouping方法连接。这种分组方式决定了Storm会以随机分配方式从源节点向目标节点发送消息 */
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReaderSpout());
		builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounterBolt(), 1).fieldsGrouping("word-normalizer", new Fields("word"));

		// 配置
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		// 运行拓扑
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Word-Count-Topology", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
	}
}