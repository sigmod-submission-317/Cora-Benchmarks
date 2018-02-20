package tpch.translated;

import org.apache.spark.api.java.JavaRDD;
import scala.Serializable;
import scala.Tuple2;
import tpch.schema.LineItem;
import tpch.schema.Supplier;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query15 {
  static class Select_1 implements Comparable<Select_1>, Serializable {
    int s_suppkey;
    String s_name;
    String s_address;
    String s_phone;
    double total_revenue;

    public Select_1(int k, String n, String a, String ph, double r){
      s_suppkey = k;
      s_name = n;
      s_address = a;
      s_phone = ph;
      total_revenue = r;
    }

    public int compareTo(Select_1 o) {
      if (s_suppkey > o.s_suppkey)
        return 1;

      if (s_suppkey < o.s_suppkey)
        return -1;

      return 0;
    }

    @Override
    public String toString(){
      return s_suppkey + ", " + s_name + ", " + s_address + ", " + s_phone + ", " + total_revenue;
    }
  }

  public static List<Select_1> run (JavaRDD<LineItem> lineitem, JavaRDD<Supplier> supplier) throws ParseException {
    Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("1996-04-01");
    Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse("1996-01-01");

    Map<Integer, Double> res0 = null;
    new HashMap<Integer, Double>();
    JavaRDD<Tuple2<Integer, Double>> rdd_0_0 = lineitem.flatMapToPair(lineitem_index -> {
      List<Tuple2<Integer, Double>> emits = new ArrayList<Tuple2<Integer, Double>>();
      if (lineitem_index.l_shipdate.before(d1) && (!lineitem_index.l_shipdate.before(d2))) emits.add(new Tuple2<Integer,Double>(lineitem_index.l_suppkey, ((1.0 - lineitem_index.l_discount) * lineitem_index.l_extendedprice)));
      return emits.iterator();
    }).reduceByKey((val1, val2) -> val2 + val1).rdd().toJavaRDD();

    Set<Map.Entry<Integer, Double>> revenue0 = res0.entrySet();

    double max_total = 0;
    max_total = Double.MIN_VALUE;
    max_total = rdd_0_0.map(revenue0_index -> revenue0_index._2()).reduce((val1, val2) -> (val1 > val2 ? val1 : val2));

    Set<Select_1> res1 = null;
    new HashSet<Select_1>();
    final double max_total_final = max_total;
    JavaRDD<Tuple2<Integer, Double>> rdd_1_0 = rdd_0_0.flatMap(revenue0_index -> {
      List<Tuple2<Integer, Double>> emits = new ArrayList<Tuple2<Integer, Double>>();
      if (revenue0_index._2() == max_total_final) emits.add(revenue0_index);
      return emits.iterator();
    });

    Set<Select_1> res2 = null;
    res2 = new HashSet<Select_1>();
    res2 = new HashSet<Select_1>(rdd_1_0.mapToPair(res1_index -> new Tuple2<Integer, Double>(res1_index._1(), res1_index._2())).join(supplier.mapToPair(supplier_index -> new Tuple2<Integer, Supplier>(supplier_index.s_suppkey, supplier_index))).map(val -> new Select_1(val._2._2().s_suppkey, val._2._2().s_name, val._2._2().s_address, val._2._2().s_phone, val._2._1())).collect());

    List<Select_1> res3 = new ArrayList<Select_1>(res2);
    java.util.Collections.sort(res3);
    return res3;
  }
}