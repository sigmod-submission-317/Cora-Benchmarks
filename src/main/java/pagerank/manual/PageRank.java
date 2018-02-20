package pagerank.manual;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import pagerank.Link;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Map;

public class PageRank {
  public static Map<String, Double> run(JavaRDD<Link> links){
    JavaPairRDD<String, Iterable<String>> grouped_links = links.mapToPair(link -> new Tuple2<String, String>(link.src_url, link.dst_url))
                                                               .groupByKey()
                                                               .cache();

    JavaPairRDD<String, Double> ranks = grouped_links.mapValues(rank -> 1.0);

    for (int i=0; i<10; i++) {
      JavaPairRDD<String, Double> contribs = grouped_links
              .join(ranks).values().flatMapToPair( tup ->{
                Iterable<String> urls = tup._1();
                double rank = tup._2();
                int size = Iterables.size(urls);

                ArrayList<Tuple2<String, Double>> scores = new ArrayList<>();
                for(String dst : urls)
                  scores.add(new Tuple2<String, Double>(dst, rank/size));
                return scores.iterator();
              });
      ranks = contribs.reduceByKey((score_1, score_2) -> score_1 + score_2).mapValues(score -> 0.15 + 0.85 * score);
    }

    return ranks.collectAsMap();
  }
}