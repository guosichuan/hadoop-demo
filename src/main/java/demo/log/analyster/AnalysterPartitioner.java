package demo.log.analyster;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysterPartitioner extends Partitioner<Text, IntWritable> {
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		if (numPartitions >= 2)// Reduce 个数，判断 loglevel 还是 logmodule 的统计，分配到不同的
								// Reduce
			if (key.toString().startsWith("logLevel::"))
				return 0;
			else if (key.toString().startsWith("moduleName::"))
				return 1;
			else
				return 0;
		else
			return 0;
	}
}