package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class OrderInversion extends Configured implements Tool {

	private final static String ASTERISK = "\0";

	public static class PartitionerTextPair extends
	Partitioner<TextPair, IntWritable> {
		@Override
		public int getPartition(TextPair key, IntWritable value,
				int numPartitions) {
			// TODO: implement getPartition such that pairs with the same first element
			//       will go to the same reducer. You can use toUnsigned as utility.
			return toUnsigned(key.getFirst().toString().hashCode())%numPartitions;
		}

		/**
		 * toUnsigned(10) = 10
		 * toUnsigned(-1) = 2147483647
		 * 
		 * @param val Value to convert
		 * @return the unsigned number with the same bits of val 
		 * */
		public static int toUnsigned(int val) {
			return val & Integer.MAX_VALUE;
		}
	}

	public static class PairMapper extends
	Mapper<LongWritable, Text, TextPair, IntWritable> {

		private HashMap<TextPair,Integer> map;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			map = new HashMap<TextPair, Integer>();
		}

		/*
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			StringTokenizer iter = new StringTokenizer(value.toString());
			if (iter.hasMoreTokens()) {
				String left =iter.nextToken();
				while (iter.hasMoreTokens()) {
					String right = iter.nextToken();
					TextPair pair = new TextPair (left, right);
					pair.getSecond().set(ASTERISK);
					if (map.containsKey(pair))
						map.put(pair, map.get(pair)+1);
					else
						map.put(pair, 1);
					pair = new TextPair (left, right);
					if (map.containsKey(pair))
						map.put(pair, map.get(pair)+1);
					else
						map.put(pair, 1);
					left = right;
				}
			}
		}
		*/
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			StringTokenizer iter = new StringTokenizer(value.toString());
			if (iter.hasMoreTokens()) {
				String left =iter.nextToken();
				while (iter.hasMoreTokens()) {
					String right = iter.nextToken();
					
					TextPair pair = new TextPair (left, right);
					if (map.containsKey(pair)) {
						map.put(pair, map.get(pair) + 1);
					} else {
						map.put(pair, 1);
					}
					
					pair = new TextPair (left, ASTERISK);
					if (map.containsKey(pair)) {
						map.put(pair, map.get(pair) + 1);
					} else {
						map.put(pair, 1);
					}
					
					left = right;
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
			IntWritable val = new IntWritable();
			for (Entry<TextPair, Integer> pair : map.entrySet()) {
				val.set(pair.getValue());
				context.write(pair.getKey(), val);
			}
		}
	}

	public static class PairReducer extends
	Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

		private DoubleWritable relative = new DoubleWritable();
		//private String left = new String();
		private int sum = 0;
		
		// TODO: implement reducer
		/*
		@Override
		public void reduce(TextPair key, // TODO: change Object to input key type
				Iterable<IntWritable> values, // TODO: change Object to input value type 
				Context context) throws IOException, InterruptedException {				
			if (key.getSecond().toString().equals(ASTERISK)) {
				if (!left.equals(key.getFirst().toString())) {
					sum = 0;
					left = key.getFirst().toString();
				}
				for (IntWritable cnt:values) sum+=cnt.get();
				freq.set((double)sum);
				context.write(key,freq);
				return;
			}
			int absolute = 0;
			for (IntWritable cnt:values) absolute+=cnt.get();
			freq.set((double)absolute/sum);
			context.write(key, freq);
		}
		*/
		
		@Override
		public void reduce(TextPair key, // TODO: change Object to input key type
				Iterable<IntWritable> values, // TODO: change Object to input value type 
				Context context) throws IOException, InterruptedException {				
			
			int absolute = 0;
			for (IntWritable cnt : values) {
				absolute += cnt.get();
			}
			
			if (key.getSecond().toString().equals(ASTERISK)) {
				sum = absolute;
				relative.set(sum);
			} else {
				relative.set((double) (absolute / sum));
			}
			
			context.write(key, relative);
		}
		
	}

	private int numReducers;
	private Path inputPath;
	private Path outputDir;

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		Job job = new Job (conf);  // TODO: define new job instead of null using conf e setting a name
		job.setJobName("OrderInversion");

		// TODO: set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// TODO: set map class and the map output key and value classes
		job.setMapperClass(PairMapper.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		// set partitioner
		job.setPartitionerClass(PartitionerTextPair.class);

		// TODO: set reduce class and the reduce output key and value classes
		job.setReducerClass(PairReducer.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(DoubleWritable.class);

		// TODO: set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// TODO: add the input file as job input (from HDFS) to the variable
		//       inputFile
		FileInputFormat.setInputPaths(job, inputPath);

		// TODO: set the output path for the job results (to HDFS) to the variable
		//       outputPath
		FileOutputFormat.setOutputPath(job, outputDir);
		// TODO: set the number of reducers using variable numberReducers
		job.setNumReduceTasks(numReducers);

		// TODO: set the jar class
		job.setJarByClass(OrderInversion.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	OrderInversion(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
			System.exit(0);
		}
		this.numReducers = Integer.parseInt(args[0]);
		this.inputPath = new Path(args[1]);
		this.outputDir = new Path(args[2]);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
		System.exit(res);
	}
}
