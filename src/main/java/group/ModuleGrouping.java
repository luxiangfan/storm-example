package group;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * @author luxiangfan
 * @date 2016-11-02, 20:36
 * <p>
 * 自定义数据流组, 可以决定哪些bolt接收哪些元组
 *
 *******************************************************************
 * 使用方式
 * <code>
 * 		builder.setBolt("word-normalizer", new WordNormalizer())
 * 			   .customGrouping("word-reader", new ModuleGrouping());
 * </code>
 *******************************************************************
 */
public class ModuleGrouping implements CustomStreamGrouping {

	int taskNum = 0;

	@Override
	public void prepare(TopologyContext context, Fields outFields, List<Integer> targetTasks) {
		taskNum = targetTasks.size();
	}

	/**
	 * 采用单词首字母字符的整数值与任务数的余数，决定接收元组的bolt
	 */
	@Override
	public List<Integer> chooseTasks(List<Object> values) {
		List<Integer> boltIds = new ArrayList<Integer>();
		if (values.size() > 0) {
			String str = values.get(0).toString();
			if (str.isEmpty()) {
				boltIds.add(0);
			} else {
				boltIds.add(str.charAt(0) % taskNum);
			}
		}
		return boltIds;
	}


}
