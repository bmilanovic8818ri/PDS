package mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class IndexMapper extends Mapper<Object, Text, Text, Text> {
		private Text pol = new Text();
		private Text name = new Text();
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			pol.set(line[0]);
			name.set(line[1]);
			context.write(pol, name);
		}
	}
	
	public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Text name : values) {
				if (first)
					first = false;
				else 
					sb.append(" ");
				sb.append(name.toString());
			}
			result.set(sb.toString());
			context.write(key,  result);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
