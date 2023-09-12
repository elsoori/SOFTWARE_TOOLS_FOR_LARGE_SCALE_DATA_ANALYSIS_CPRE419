
/**
  *************************************************
  *************************************************
  * Cpr E 419 - Lab 2 Experiment 2****************
  * Eranda Sooriyarachchi (eranda@iastate.edu)********
  Jyothi Prasanth Durairaj Rajeswari (jyothi@iastate.edu)********
 *****************************************************
  *****************************************
  *****************************************
  */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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

public class Driver {

	public static void main(String[] args) throws Exception {

		// Change following paths accordingly
		String input = "/lab2/gutenburg";
		String temp = "/user/cpre419/lab2/exp2/temp";
		String output = "/user/cpre419/lab2/output/exp2";

		// The number of reduce tasks
		int reduce_tasks = 2;

		Configuration conf = new Configuration();

		// Create job for round 1
		Job job_one = Job.getInstance(conf, "Driver Program Round One");

		// Attach the job to this Driver
		job_one.setJarByClass(Driver.class);

		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);

		// The datatype of the mapper output Key, Value
		// job_one.setMapOutputKeyClass(Text.class);
		// job_one.setMapOutputValueClass(IntWritable.class);

		// The datatype of the reducer output Key, Value
		job_one.setOutputKeyClass(Text.class);
		job_one.setOutputValueClass(IntWritable.class);

		// The class that provides the map method
		job_one.setMapperClass(Map_One.class);

		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);

		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);

		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);

		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input));

		// This is legal
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path));

		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));

		// This is not allowed
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path));

		// Run the job
		job_one.waitForCompletion(true);

		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1

		Job job_two = Job.getInstance(conf, "Driver Program Round Two");
		job_two.setJarByClass(Driver.class);
		job_two.setNumReduceTasks(reduce_tasks);

		// Should be match with the output datatype of mapper and reducer
		// job_two.setMapOutputKeyClass(Text.class);
		// job_two.setMapOutputValueClass(IntWritable.class);
		job_two.setOutputKeyClass(Text.class);
		job_two.setOutputValueClass(Text.class);

		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class);
		job_two.setReducerClass(Reduce_Two.class);

		job_two.setInputFormatClass(TextInputFormat.class);
		job_two.setOutputFormatClass(TextOutputFormat.class);

		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp));
		FileOutputFormat.setOutputPath(job_two, new Path(output));

		// Run the job
		job_two.waitForCompletion(true);

		/**
		 * **************************************
		 * **************************************
		 * FILL IN CODE FOR MORE JOBS IF YOU NEED
		 * **************************************
		 * **************************************
		 */

	}

	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text
	// (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can
	// be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and
	// IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final IntWritable one = new IntWritable(1);
		private final Text bigram = new Text();

		// The map method
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			String prev_word = null;
			// Tokenize to get the individual words
			StringTokenizer tokens = new StringTokenizer(line);

			while (tokens.hasMoreTokens()) {
				String s = tokens.nextToken();

				// Clean up the word
				String cleanedWord = s.toLowerCase().replaceAll("[^a-z0-9]", " ").replaceAll("\\s+", " ");

				// Split the cleaned-up word into individual words
				String[] words = cleanedWord.split("\\s+");

				// Emit bigrams for each pair of consecutive words, skipping empty and
				// punctuation-only words

				for (String word : words) {

					if (prev_word != null && !prev_word.equals(".") && !word.equals(".") && !prev_word.equals("") &&
							!word.equals("")) {
						bigram.set(prev_word + " " + word);
						context.write(bigram, one);
					}
					prev_word = word;
				}
			} // End while

			// Use context.write to emit values
		}
	}

	// The Reduce class
	// The key is Text and must match the datatype of the output key of the map
	// method
	// The value is IntWritable and also must match the datatype of the output
	// value of the map method
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable> {

		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			// sum the values and regurgitate
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));

		}
	}

	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text> {

		private final Text textkey = new Text("key");

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			context.write(textkey, value);

		}
	}

	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Create a priority queue to keep track of the top 10 bigrams by value
			Map<String, Integer> maxheap = new HashMap<String, Integer>();

			// Iterate through the input values
			for (Text value : values) {
				// Split the value into tokens using a tab delimiter
				String[] tokens = value.toString().split("\t");
				// Extract the bigram and count from the tokens
				String bigram = tokens[0];
				int count = Integer.parseInt(tokens[1]);

				// If the map has fewer than 10 entries, add the current entry
				if (maxheap.size() < 10) {
					maxheap.put(bigram, count);
				}
				// Otherwise, if the count of the current entry is greater than the minimum
				// count in the map,
				// remove the minimum entry and add the current entry
				else {
					String minKey = null;
					int minValue = Integer.MAX_VALUE;
					for (Map.Entry<String, Integer> entry : maxheap.entrySet()) {
						if (entry.getValue() < minValue) {
							minKey = entry.getKey();
							minValue = entry.getValue();
						}
					}
					if (count > minValue) {
						maxheap.remove(minKey);
						maxheap.put(bigram, count);
					}
				}
			}

			// Iterate through the map
			for (Map.Entry<String, Integer> entry : maxheap.entrySet()) {
				// Write each entry to the context object
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}
}