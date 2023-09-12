import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Lab6 {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: <input> <output>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("Lab6 experiment1");

		JavaSparkContext context = new JavaSparkContext(sparkConf);

		// loading the githuv.csv
		JavaRDD<String> file = context.textFile(args[0]);

		// map the github by (key, value)=(language, line)
		JavaPairRDD<String, String> github = file.mapToPair(
				new PairFunction<String, String, String>() {
					public Tuple2<String, String> call(String s) {
						String[] data = s.split(",");
						return new Tuple2<String, String>(data[1], s);
					}
				});

		// grouping the language
		JavaPairRDD<String, Iterable<String>> lang_grouping = github.groupByKey();

		// formatting the given language in given format
		JavaPairRDD<Long, String> formatted_string = lang_grouping.mapToPair(
				new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
					public Tuple2<Long, String> call(
							Tuple2<String, Iterable<String>> values) {
						// # of record
						long records = 0;
						// max star
						long maximum_stars = 0;
						// name of the repositories holds the maximum_stars
						String repositories = "";
						// name of the language
						String language = "";
						for (String s : values._2) {
							String[] data = s.split(",");
							records++;
							if (Long.valueOf(data[12]) > maximum_stars) {
								maximum_stars = Long.valueOf(data[12]);
								repositories = data[0];
								language = data[1];
							}
						}
						return new Tuple2<Long, String>(
								records,
								language + " " + records + " " + repositories + " " + maximum_stars);
					}
				});

		// sort
		JavaPairRDD<Long, String> sorted_output = formatted_string.sortByKey(false);

		// save
		sorted_output.values().saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}