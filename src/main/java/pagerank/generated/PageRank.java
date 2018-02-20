package pagerank.generated;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import pagerank.Link;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PageRank {
  public static Map<String, Double> run(JavaRDD<Link> links){
    Map<String, Double> ranks = null;
    ranks = new HashMap<>();
    HashMap<String, ArrayList<String>> grouped_links = null;
    grouped_links = new HashMap<>();

    JavaPairRDD<String, ArrayList<String>> rdd_0_0 = links.mapToPair(links_index -> new Tuple2<String, ArrayList<String>>(links_index.src_url, new ArrayList<String>(Arrays.asList(links_index.dst_url)))).reduceByKey((val1, val2) -> { val1.addAll(val2); return val1; });

    JavaPairRDD<String, Double> rdd_1_0 = rdd_0_0.mapValues(rdd_1_0_index -> 1.0);

    for (int i=0; i<10; i++) {
      HashMap<String, Double> contrib = null;
      contrib = new HashMap<>();
      JavaPairRDD<String, Double> rdd_2_0 = rdd_0_0.join(rdd_1_0).flatMapToPair(val -> val._2()._1().stream().map(e -> new Tuple2<String, Double>(e, val._2()._2() / val._2()._1().size())).iterator()).reduceByKey((val1, val2) -> (val2 + val1));

      rdd_1_0 = rdd_2_0.mapToPair(val -> new Tuple2<String, Double>(val._1(), ((0.85 * val._2()) + 0.15)));
    }
    ranks = rdd_1_0.collectAsMap();

    return ranks;
  }
}