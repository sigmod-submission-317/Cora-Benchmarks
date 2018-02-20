package pagerank;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import pagerank.manual.PageRank;

public class Driver {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .appName("PageRank")
            .getOrCreate();

    JavaRDD<Link> lines = spark.read().textFile(args[0]).javaRDD().map(line -> { String[] parts = line.split("\\s+"); return new Link(parts[0], parts[1]); });

    if (args[1].equals("man")){
      PageRank.run(lines);
    }
    else {
      pagerank.generated.PageRank.run(lines);
    }
  }
}