
/**************Jyothi Prasanth Durairaj Rajeswari(jyothi@iastate.edu) &  Eranda Sooriyarachchi (eranda@iastate.edu)*******************/
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import javax.lang.model.element.ElementVisitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class CustomSort {

  public static void main(String[] args) throws Exception {
    String input = "/lab3/input-500k";
    String temp = "/eranda/b/temp/";
    String output = "/eranda/b/output/";
    ///////////////////////////////////////////////////
    ///////////// First Round MapReduce ///////////////
    ////// where you might want to do some sampling ///
    ///////////////////////////////////////////////////
    int ReducerNumber = 10;

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Exp2");

    job.setJarByClass(CustomSort.class);
    job.setNumReduceTasks(ReducerNumber);

    job.setMapperClass(Map_One.class);
    // job.setCombinerClass(combinerOne.class);
    job.setReducerClass(Reduce_One.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(input));

    FileOutputFormat.setOutputPath(job, new Path(temp));

    job.waitForCompletion(true);

    ///////////////////////////////////////////////////
    ///////////// Second Round MapReduce //////////////
    ///////////////////////////////////////////////////
    Job job_two = Job.getInstance(conf, "Round Two");
    job_two.setJarByClass(CustomSort.class);

    conf.setInt("Count", 0);
    // Providing the number of reducers for the second round
    ReducerNumber = 10;
    job_two.setNumReduceTasks(ReducerNumber);

    // Partitioner is my custom partitioner class
    job_two.setPartitionerClass(MyPartitioner.class);

    // Should be match with the output datatype of mapper and reducer
    job_two.setMapOutputKeyClass(Text.class);
    job_two.setMapOutputValueClass(Text.class);

    job_two.setOutputKeyClass(Text.class);
    job_two.setOutputValueClass(Text.class);

    job_two.setMapperClass(Map_Two.class);
    job_two.setReducerClass(Reduce_Two.class);

    // Input and output format class
    job_two.setInputFormatClass(KeyValueTextInputFormat.class);
    job_two.setOutputFormatClass(TextOutputFormat.class);

    // The output of previous job set as input of the next
    FileInputFormat.addInputPath(job_two, new Path(temp));
    FileOutputFormat.setOutputPath(job_two, new Path(output));

    // Run the job
    System.exit(job_two.waitForCompletion(true) ? 0 : 1);
  }

  public static class Map_One extends Mapper<Text, Text, Text, Text> {

    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  public static class Reduce_One extends Reducer<Text, Text, Text, Text> {

    private Text line = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        // Since the length of input file has a length of 15 and 40
        if (key.toString().length() != 15 && value.toString().length() != 40)

        {
          return;
        }
        context.write(key, value);
      }
    }
  }

  // Compare each input key with the boundaries we get from the first round
  // And add the partitioner information in the end of values
  public static class Map_Two extends Mapper<Text, Text, Text, Text> {

    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      // For instance, partitionFile0 to partitionFile8 are the discovered boundaries,
      // based on which you add the No ID 0 to 9 at the end of value
      // How to find the boundaries is your job for this experiment
      String String = key.toString();
      String val = value.toString();
      // Since the length of input file has a length of 15 and 40
      if (String.length() != 15 && val.length() != 40) {
        return;
      }

      // Sorting Alphabetically from A to P ,so we used
      // compareTo Method for the strings from the input file

      if (String.compareTo("A") <= 0)// For A
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(0)));
      } else if (String.compareTo("B") <= 0) // For B
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(1)));
      } else if (String.compareTo("C") <= 0) // For C
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(2)));
      } else if (String.compareTo("D ") <= 0) // For D
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(3)));
      } else if (String.compareTo("E") <= 0) // For E
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(4)));
      } else if (String.compareTo("F") <= 0) // For F
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(5)));
      } else if (String.compareTo("G") <= 0) // For G
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(6)));
      } else if (String.compareTo("H") <= 0) // For H
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(7)));
      } else if (String.compareTo("I") <= 0) // For I
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(8)));
      } else if (String.compareTo("J") > 0) // For J
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(9)));
      } else if (String.compareTo("K") > 0)// For K
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(9)));
      } else if (String.compareTo("L") > 0) // For L
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(9)));
      } else if (String.compareTo("M") > 0) // For M
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(9)));
      } else if (String.compareTo("N") > 0) // For N
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(9)));
      } else if (String.compareTo("O") > 0) // For O
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(9)));
      } else if (String.compareTo("P") > 0) // For P
      {
        context.write(
            new Text(String),
            new Text(value.toString() + ";" + Integer.toString(9)));
      }
    }
  }

  public static class Reduce_Two extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        String[] temp_str = value.toString().split(";");
        context.write(key, new Text(temp_str[0]));
      }
    }
  }

  // For the output, input values are extracted

  public static class MyPartitioner extends Partitioner<Text, Text> {

    public int getPartition(Text key, Text value, int numReduceTasks) {
      String[] Sort = value.toString().split(";");
      return Integer.parseInt(Sort[1]);
    }
  }
}
