
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class Graph {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: Wordcnt <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Firewall in Spark");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> patent_file = context.textFile(args[0]);
		JavaRDD<String> patents = patent_file.filter(x -> !x.isEmpty());

		JavaPairRDD<String, String> num_edges = patents
				.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
					public Iterator<Tuple2<String, String>> call(String s) {

						String starting = s.split("\\s+")[0];
						String ending = s.split("\\s+")[1];

						return Arrays
								.asList(new Tuple2<String, String>(starting, ending),
										new Tuple2<String, String>(ending, starting))
								.iterator();
					}
				});

		JavaPairRDD<String, Iterable<String>> tr_ = num_edges.groupByKey();

		JavaPairRDD<Tuple2<String, String>, String> possibletr_ = tr_
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, // for input value
						Tuple2<String, String>, // for key o/p
						String // for value o/p
				>() {
					public Iterator<Tuple2<Tuple2<String, String>, String>> call(Tuple2<String, Iterable<String>> s) {

						Iterable<String> values = s._2;
						// here we are assuming that nodes doesn't have Id as zero
						List<Tuple2<Tuple2<String, String>, String>> res = new ArrayList<Tuple2<Tuple2<String, String>, String>>();

						// searching the possible tr_.
						for (String value : values) {
							Tuple2<String, String> k_2 = new Tuple2<String, String>(s._1, value);
							Tuple2<Tuple2<String, String>, String> k_2v2 = new Tuple2<Tuple2<String, String>, String>(
									k_2,
									"");
							res.add(k_2v2);
						}

						// Since, RDD's values are immutable, we have to copy the values to cp_values
						List<String> cp_values = new ArrayList<String>();
						for (String item : values) {
							cp_values.add(item);
						}
						Collections.sort(cp_values);

						// Generate possible tr_.
						for (int i = 0; i < cp_values.size() - 1; ++i) {
							for (int j = i + 1; j < cp_values.size(); ++j) {
								Tuple2<String, String> k2 = new Tuple2<String, String>(cp_values.get(i),
										cp_values.get(j));
								Tuple2<Tuple2<String, String>, String> k2v2 = new Tuple2<Tuple2<String, String>, String>(
										k2, s._1);
								res.add(k2v2);
							}
						}

						return res.iterator();
					}
				});

		JavaPairRDD<Tuple2<String, String>, Iterable<String>> tr_Grouped = possibletr_.groupByKey();

		JavaRDD<Tuple3<String, String, String>> tri_with_dups = tr_Grouped
				.flatMap(new FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<String>>, // for input
						Tuple3<String, String, String> // for output
				>() {

					public Iterator<Tuple3<String, String, String>> call(
							Tuple2<Tuple2<String, String>, Iterable<String>> s) {

						// Since, 0 is a fake node, and hence it does not exist
						Tuple2<String, String> key = s._1;
						Iterable<String> values = s._2;
						// Therefore, we assume that no node has an ID of zero

						List<String> list = new ArrayList<String>();
						boolean seen_zero = false;
						for (String node : values) {
							if (node.equals("")) {
								seen_zero = true;
							} else {
								list.add(node);
							}
						}

						List<Tuple3<String, String, String>> res = new ArrayList<Tuple3<String, String, String>>();
						if (seen_zero) {
							if (list.isEmpty()) // If no triangles are found here we return nothing

							{
								return res.iterator(); // res if triangles found
							}

							for (String node : list) {
								String[] a_triangle = { key._1, key._2, node };
								Arrays.sort(a_triangle);
								Tuple3<String, String, String> t3 = new Tuple3<String, String, String>(a_triangle[0],
										a_triangle[1], a_triangle[2]);
								res.add(t3);
							}
						} else {
							// no triangles found

							return res.iterator();
						}

						return res.iterator();
					}
				});

		JavaRDD<Tuple3<String, String, String>> tri_with_unique_vals = tri_with_dups.distinct();

		System.out.println("*******tri_with_unique_vals*******");
		List<Tuple3<String, String, String>> output = tri_with_unique_vals.collect();
		for (Tuple3<String, String, String> t3 : output) {

			System.out.println("t3=" + t3);
		}

		JavaPairRDD<String, Integer> one = tri_with_unique_vals
				.mapToPair(new PairFunction<Tuple3<String, String, String>, String, Integer>() {
					public Tuple2<String, Integer> call(Tuple3<String, String, String> t) {

						return new Tuple2<String, Integer>("triangle", 1);
					}
				});

		JavaPairRDD<String, Integer> cnt = one.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);

		JavaRDD<Integer> final_ = cnt.map(new Function<Tuple2<String, Integer>, Integer>() {
			public Integer call(Tuple2<String, Integer> t) {
				return t._2;
			}
		});

		final_.saveAsTextFile(args[1]);
		context.stop();
		context.close();
	}
}
