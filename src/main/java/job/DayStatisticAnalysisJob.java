package job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mapper.DayStatisticAnalysisMapper;
import reducer.DayStatisticAnalysisReducer;

public class DayStatisticAnalysisJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
            System.err.println("Parameter Errors! Usage <inputPath...> <outputPath>");
            System.exit(-1);
        }

        Path outputPath = new Path(args[1]);

        Configuration conf = new Configuration();
        String jobName = DayStatisticAnalysisJob.class.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(DayStatisticAnalysisJob.class);

        // 设置mr的输入参数
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(DayStatisticAnalysisMapper.class);
        // 设置mr的输出参数
        outputPath.getFileSystem(conf).delete(outputPath, true);    // 避免job在运行的时候出现输出目录已经存在的异常
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setReducerClass(DayStatisticAnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
	}
}
