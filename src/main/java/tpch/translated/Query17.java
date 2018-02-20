package tpch.translated;

import org.apache.spark.api.java.JavaRDD;
import scala.Serializable;
import scala.Tuple2;
import tpch.schema.LineItem;
import tpch.schema.Part;

import java.util.*;

public class Query17 {
  static class Select_1 implements Serializable{
    double sum;
    int count;
    public Select_1(double s, int c){
      sum = s; count = c;
    }
  }

  static class Select_2 implements  Serializable{
    int p_partkey;
    double avg;
    public Select_2(int k, double a){
      p_partkey = k; avg = a;
    }
  }

  static class Select_3 implements Serializable{
    public double quantity;
    public double average;
    public double extendedprice;
    public Select_3(double q, double a, double e) {
      quantity = q;
      average = a;
      extendedprice = e;
    }
  }

  public static double run (JavaRDD<LineItem> lineitem, JavaRDD<Part> parts) {
    String brand = null;
    brand = "Brand#23";
    String container = null;
    container = "MED BOX";

    Set<Integer> res0 = null;
    res0 = new HashSet<Integer>();
    final String brand_final = brand;
    final String container_final = container;
    JavaRDD<Integer> rdd_0_0 = parts.flatMap(part_index -> {
      List<Integer> emits = new ArrayList<Integer>();
      if ((part_index.p_container.equals(container_final) && part_index.p_brand.equals(brand_final))) emits.add(part_index.p_partkey);
      return emits.iterator();
    });

    Map<Integer, Select_1> res1 = null;
    res1 = new HashMap<Integer, Select_1>();
    JavaRDD<Tuple2<Integer, Select_1>> rdd_1_0 = lineitem.mapToPair(lineItem_index -> new Tuple2<Integer, Select_1>(lineItem_index.l_partkey, new Select_1(lineItem_index.l_quantity, 1))).reduceByKey((val1, val2) -> new Select_1(val1.sum + val2.sum, val1.count + val2.count)).rdd().toJavaRDD();

    Set<Map.Entry<Integer, Select_1>> res2 = null;
    res2 = res1.entrySet();

    Set<Select_2> res3 = null;
    res3 = new HashSet<Select_2>();
    JavaRDD<Select_2> rdd_2_0 = rdd_1_0.map(res2_index -> new Select_2(res2_index._1(), ((res2_index._2().sum / res2_index._2().count) * 0.2)));

    Set<Select_2> res4 = null;
    res4 = new HashSet<Select_2>();
    JavaRDD<Select_2> rdd_3_0 = rdd_2_0.mapToPair(res3_index -> new Tuple2<Integer, Select_2>(res3_index.p_partkey, res3_index)).join(rdd_0_0.mapToPair(res0_index -> new Tuple2<Integer, Integer>(res0_index, res0_index))).map(val -> val._2()._1());

    Set<Select_3> res5 = null;
    res5 = new HashSet<Select_3>();
    JavaRDD<Select_3> rdd_4_0 = lineitem.mapToPair(lineItem_index -> new Tuple2<Integer, LineItem>(lineItem_index.l_partkey, lineItem_index)).join(rdd_2_0.mapToPair(val -> new Tuple2<Integer, Select_2>(val.p_partkey, val))).map(val -> new Select_3(val._2()._1().l_quantity, val._2()._2().avg, val._2()._1().l_extendedprice));

    double avg_yearly = 0;
    avg_yearly = 0;
    avg_yearly = rdd_4_0.flatMap(res4_index -> {
      List<Double> emits = new ArrayList<Double>();
      if (res4_index.quantity < res4_index.average) emits.add(res4_index.extendedprice);
      return emits.iterator();
    }).reduce((val1, val2) -> val1 + val2);

    avg_yearly = avg_yearly / 7.0;
    return avg_yearly;
  }
}