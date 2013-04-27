package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Pair extends Configured implements Tool {

	public static class PairMapper 
		extends Mapper<LongWritable, // TODO: change Object to input key type
					Text, // TODO: change Object to input value type
					TextPair, // TODO: change Object to output key type
					IntWritable> { // TODO: change Object to output value type

		private static final IntWritable ONE = new IntWritable(1);
		private TextPair pair = new TextPair();

		// TODO: implement mapper
		@Override
		public void map(LongWritable key, // TODO: change Object to input key type
				Text value, // TODO: change Object to input value type
				Context context)
				throws java.io.IOException, InterruptedException {
			
			StringTokenizer iter = new StringTokenizer(value.toString());
			if (iter.hasMoreTokens()) {
				String left = iter.nextToken();
				while (iter.hasMoreTokens()) {
					String right = iter.nextToken();
					pair.set(left, right);
					context.write(pair, ONE);
					left = right;
				}
			}	
		}

	}

	public static class PairReducer
		extends Reducer<TextPair, // TODO: change Object to input key type
					IntWritable, // TODO: change Object to input value type
					TextPair, // TODO: change Object to output key type
					IntWritable> { // TODO: change Object to output value type

		private IntWritable sumWritable = new IntWritable();

		// TODO: implement reducer
		@Override
		public void reduce(TextPair key, // TODO: change Object to input key type
				Iterable<IntWritable> values, // TODO: change Object to input value type 
				Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable cnt : values) {
				sum += cnt.get();
			}
			sumWritable.set(sum);
			context.write(key, sumWritable);
		}
	}

	private int numReducers;
	private Path inputPath;
	private Path outputDir;

	public Pair(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		this.numReducers = Integer.parseInt(args[0]);
		this.inputPath = new Path(args[1]);
		this.outputDir = new Path(args[2]);
	}


	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		// TODO: define new job instead of null using conf e setting a name
		Job job = new Job (conf);
		job.setJobName("Pair");

		// TODO: set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// TODO: set map class and the map output key and value classes
		job.setMapperClass(PairMapper.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		// set combiner class
		job.setCombinerClass(PairReducer.class);

		// TODO: set reduce class and the reduce output key and value classes
		job.setReducerClass(PairReducer.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(IntWritable.class);

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
		job.setJarByClass(Pair.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Pair(args), args);
		System.exit(res);
	}
}