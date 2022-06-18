package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Tuple {

	public static class MinMaxCountTuple implements Writable {
		private int min = 0;
		private int max = 0;
		private int count = 0;
		
		public MinMaxCountTuple() {}
		
		public MinMaxCountTuple(int temperature) {
			this.min = temperature;
			this.max = temperature;
			this.count = 1;
		}
		
		public int getMin() { return min; }
		public int getMax() { return max; }
		public int getCount() { return count; }
		public void setMin(int min) { this.min = min; }
		public void setMax(int max) { this.max = max; }
		public void setCount(int count) { this.count = count; }
		
		@Override
		public void readFields(DataInput in) throws IOException {
			min = in.readInt();
			max = in.readInt();
			count = in.readInt();			
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(min);
			out.writeInt(max);
			out.writeInt(count);
		}
		
		public String toString() {
			return "min: " + min + ", max: " + max + ", count: " + count;
		}
		
	}
	
	public static class TupleMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {
		private Text month = new Text();
		private MinMaxCountTuple outTuple = new MinMaxCountTuple();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(",");
			month.set(line[1].substring(4,6));
			int temperature = Integer.parseInt(line[3]);
			
			outTuple.setMin(temperature);
			outTuple.setMax(temperature);
			outTuple.setCount(1);
			//outTuple = new MinMaxCountTuple(temperature);
			
			context.write(month, outTuple);
		}
	}
	
	public static class TupleReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
		private MinMaxCountTuple result = new MinMaxCountTuple();		
		
		public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
			result.setCount(0);
			result.setMax(Integer.MIN_VALUE);
			result.setMin(Integer.MAX_VALUE);
			int sum = 0;
			
			for (MinMaxCountTuple value : values) {
				if (value.getMin() < result.getMin()) {
					result.setMin(value.getMin());
				}
				if (value.getMax() > result.getMax()) {
					result.setMax(value.getMax());
				}
				sum += value.getCount();
			}
			
			result.setCount(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "min max count temperature");
		job.setJarByClass(Tuple.class);
		job.setMapperClass(TupleMapper.class);
		job.setCombinerClass(TupleReducer.class);
		job.setReducerClass(TupleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
 