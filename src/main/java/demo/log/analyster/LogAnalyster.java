package demo.log.analyster;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogAnalyster extends Configured implements Tool {

	public static void main(String[] args) {
		try {
			int res;
			res = ToolRunner.run(new Configuration(), new LogAnalyster(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		// verify input path and output path
		if (args == null || args.length < 2) {
			System.out.println("need inputpath and outputpath");
			return 1;
		}
		
		String inputpath = args[0];
		String outputpath = args[1];
		File inputdir = new File(inputpath);
		File outputdir = new File(outputpath);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.out.println("inputpath not exist or isn't dir!");
			return 0;
		}
		if (!outputdir.exists()) {
			new File(outputpath).mkdirs();
		}

		// setup the hdfs path for running mapreduce job
		String shortin = args[0];
		String shortout = args[1];
		if (shortin.indexOf(File.separator) >= 0)
			shortin = shortin.substring(shortin.lastIndexOf(File.separator));
		if (shortout.indexOf(File.separator) >= 0)
			shortout = shortout.substring(shortout.lastIndexOf(File.separator));
		SimpleDateFormat formater = new SimpleDateFormat("yyyy.MM.dd.HH.mm");
		shortout = new StringBuffer(shortout).append("-").append(formater.format(new Date())).toString();
		if (!shortin.startsWith("/"))
			shortin = "/" + shortin;
		if (!shortout.startsWith("/"))
			shortout = "/" + shortout;
		shortin = "/user/hm/dfs" + shortin;
		shortout = "/user/hm/dfs" + shortout;

		// initialize yarn job based on hadoop 0.23.x
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(LogAnalyster.class);
		job.setJobName("analysisjob");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(LogAnalyster.class);
		job.setMapperClass(AnalysterMapper.class);
		job.setCombinerClass(AnalysterReducer.class);
		job.setReducerClass(AnalysterReducer.class);
		job.setPartitionerClass(AnalysterPartitioner.class);
		job.setNumReduceTasks(2);
		FileInputFormat.setInputPaths(job, new Path(shortin));
		FileOutputFormat.setOutputPath(job, new Path(shortout));

		// running the job
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		job.waitForCompletion(true);
		Date endTime = new Date();
		System.out.println("Job ended: " + endTime);
		System.out.println("The time interval of analysis job: " + (endTime.getTime() - startTime.getTime()) / 1000 + " seconds.");
		// delete temp input and output file
		// fileSys.copyToLocalFile(new Path(shortout), new Path(your_local_output_path));
		// fileSys.delete(new Path(shortin), true);
		// fileSys.delete(new Path(shortout), true);
		return 0;
	}
}