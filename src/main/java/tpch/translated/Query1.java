package tpch.translated;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Serializable;
import scala.Tuple2;
import tpch.schema.LineItem;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query1 {
  static class Select_1 implements Serializable {
    public char l_returnflag;
    public char l_linestatus;
    public Select_1(char rf, char ls){
      l_returnflag = rf;
      l_linestatus = ls;
    }
    @Override
    public boolean equals(Object o){
      if (o != null && o instanceof Select_1){
        return l_linestatus == ((Select_1) o).l_linestatus && l_returnflag == ((Select_1) o).l_returnflag;
      }
      return false;
    }
    @Override
    public int hashCode(){
      return 1;
    }
  }

  static class Select_2 implements Serializable {
    public double sum_qty;
    public double sum_base_price;
    public double sum_disc_price;
    public double sum_charge;
    public double avg_disc;
    public int count_order;
    public Select_2(double sq, double sbp, double sdp, double sc, double avgd, int co){
      sum_qty = sq;
      sum_disc_price = sdp;
      sum_charge = sc;
      avg_disc = avgd;
      count_order = co;
    }
  }

  static class Select_3 implements Comparable<Select_3>, Serializable {
    public char l_returnflag;
    public char l_linestatus;
    public double sum_qty;
    public double sum_base_price;
    public double sum_disc_price;
    public double sum_charge;
    public double avg_qty;
    public double avg_price;
    public double avg_disc;
    public int count_order;

    public Select_3(char rf, char ls, double sq, double sbp, double sdp, double sc, double avgq, double avgp, double avgd, int co){
      l_returnflag = rf;
      l_linestatus = ls;
      sum_qty = sq;
      sum_base_price = sbp;
      sum_disc_price = sdp;
      sum_charge = sc;
      avg_qty = avgq;
      avg_price = avgp;
      avg_disc = avgd;
      count_order = co;
    }

    public int compareTo(Select_3 o) {
      if (l_returnflag > o.l_returnflag)
        return 1;
      else if (l_returnflag < o.l_returnflag)
        return -1;
      else if (l_linestatus > o.l_linestatus)
        return 1;
      else if (l_linestatus < o.l_linestatus)
        return -1;
      else
        return 0;
    }

    @Override
    public String toString(){
      return "[" + l_returnflag + ", " + l_linestatus + ", " + sum_qty + ", " + sum_base_price + ", " + sum_disc_price + ", " + sum_charge + ", " + avg_qty + ", " + avg_price + ", " + avg_disc + ", " + count_order + "]\n";
    }
  }

  public static List<Select_3> run (JavaRDD<LineItem> lineitem) throws ParseException {
    Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("1998-09-02");

    Map<Select_1,Select_2> res1 = null;
    res1 = new HashMap<Select_1,Select_2>();
    JavaPairRDD<Select_1,Select_2> rdd_0_0 = lineitem.flatMapToPair(lineItem_index -> {
      List<Tuple2<Select_1,Select_2>> emits = new ArrayList<Tuple2<Select_1,Select_2>>();
      if (lineItem_index.l_shipdate.before(d1)) emits.add(new Tuple2<Select_1, Select_2>(new Select_1(lineItem_index.l_returnflag, lineItem_index.l_linestatus), new Select_2(lineItem_index.l_quantity, lineItem_index.l_extendedprice, (lineItem_index.l_extendedprice * (1 - lineItem_index.l_discount)), ((1 + lineItem_index.l_tax) * lineItem_index.l_extendedprice * (1 - lineItem_index.l_discount)), lineItem_index.l_discount, 1)));
      return emits.iterator();
    }).reduceByKey((val1, val2) -> new Select_2((val1.sum_qty + val2.sum_qty), (val2.sum_base_price + val1.sum_base_price), (val2.sum_disc_price + val1.sum_disc_price), (val1.sum_charge + val2.sum_charge), (val2.avg_disc + val1.avg_disc), (val2.count_order + val1.count_order)));

    Set<Map.Entry<Select_1, Select_2>> res2 = null;
    res2 = res1.entrySet();

    Set<Select_3> res3 = null;
    res3 = new HashSet<Select_3>();
    res3 = new HashSet<>(rdd_0_0.map(res1_index -> new Select_3(res1_index._1().l_returnflag, res1_index._1().l_linestatus, res1_index._2().sum_qty, res1_index._2().sum_base_price, res1_index._2().sum_disc_price, res1_index._2().sum_charge, res1_index._2().sum_qty / res1_index._2().count_order, res1_index._2().sum_base_price / res1_index._2().count_order, res1_index._2().avg_disc / res1_index._2().count_order, res1_index._2().count_order)).collect());

    List<Select_3> res4 = new ArrayList<Select_3>(res3);
    java.util.Collections.sort(res4);
    return res4;
  }
}