package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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


public class Stripes extends Configured implements Tool {

	private int numReducers;
	private Path inputPath;
	private Path outputDir;

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		// TODO: define new job instead of null using conf e setting a name
		Job job = new Job (conf);
		job.setJobName("Stripes");

		// TODO: set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// TODO: set map class and the map output key and value classes
		job.setMapperClass(StripesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringToIntAssociativeArray.class);

		// TODO: set reduce class and the reduce output key and value classes
		//job.setReducerClass(StripesReducer.class);
		job.setReducerClass(StripesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringToIntAssociativeArray.class);

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
		job.setJarByClass(Stripes.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public Stripes (String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
			System.exit(0);
		}
		this.numReducers = Integer.parseInt(args[0]);
		this.inputPath = new Path(args[1]);
		this.outputDir = new Path(args[2]);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
		System.exit(res);
	}
}

class StripesMapper
	extends Mapper<LongWritable,   // TODO: change Object to input key type
				Text,   // TODO: change Object to input value type
				Text,   // TODO: change Object to output key type
				StringToIntAssociativeArray> { // TODO: change Object to output value type

	//private StringToIntAssociativeArray stripe = new StringToIntAssociativeArray();
	private Map<String, StringToIntAssociativeArray> stripes = new HashMap<String, StringToIntAssociativeArray>();
	private Text word = new Text();
	
	/*
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
				stripe.set(right, 1);
				word.set(left);
				context.write(word, stripe);
				stripe.clear();
				left = right;
			}
		}    
	}
	*/
	
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
				
				StringToIntAssociativeArray s;
				if ((s = stripes.get(left)) != null) {
					Integer val;
					if ((val = s.get(right)) != null) {
						s.set(right, val + 1);
					} else {
						s.set(right, 1);
					}
				} else {
					s = new StringToIntAssociativeArray();
					s.set(right, 1);
				}
				stripes.put(left, s);
				
				left = right;
			}
		}
		
		for (Entry<String, StringToIntAssociativeArray> pair : stripes.entrySet()) {
			word.set(pair.getKey());
			context.write(word, pair.getValue());
		}
	}
}

class StripesReducer
	extends Reducer<Text,   // TODO: change Object to input key type
					StringToIntAssociativeArray,   // TODO: change Object to input value type
					Text,   // TODO: change Object to output key type
					StringToIntAssociativeArray> { // TODO: change Object to output value type

	private StringToIntAssociativeArray output=new StringToIntAssociativeArray();

	@Override
	public void reduce(Text key, // TODO: change Object to input key type
			Iterable<StringToIntAssociativeArray> values, // TODO: change Object to input value type 
			Context context) throws IOException, InterruptedException {
		
		for (StringToIntAssociativeArray stripe : values) 	
			for (Entry<String,Integer> pair : stripe.entrySet()) {
				Integer val;
				if ((val = output.get(pair.getKey())) != null) {
					output.set(pair.getKey(), val + pair.getValue());
				} else {
					output.set(pair.getKey(), pair.getValue());
				}
			}
		context.write(key, output);
		output.clear();
	}
}