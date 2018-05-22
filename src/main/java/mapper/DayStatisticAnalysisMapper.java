package mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DayStatisticAnalysisMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private SimpleDateFormat sdf;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\\|");
		String time = fields[0];
		String target = fields[1];
		String channel = fields[2];
		String districtServer = fields[3];
		String counts = fields[4];
		try {
			// 将日期转化为天
			Date d = sdf.parse(time);
			time = sdf.format(d);
			String s = time + "\t" + target + "\t" + channel + "\t" + districtServer;
			Long valueCount = Long.parseLong(counts);
			context.write(new Text(s), new LongWritable(valueCount));
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		sdf = new SimpleDateFormat("yyyy-MM-dd");
	}

}
