package tpch.seq;

import scala.Serializable;
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
      if (o != null && o instanceof Query1.Select_1){
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

  public static List<Select_3> run (Set<LineItem> lineitem) throws ParseException {
    Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("1998-09-02");

    Map<Select_1,Select_2> res1 = new HashMap<Select_1,Select_2>();
    for (LineItem l : lineitem) {
      if (!l.l_shipdate.before(d1)) {
        Select_1 key = new Select_1(l.l_returnflag, l.l_linestatus);
        if (!res1.containsKey(key))
          res1.put(key, new Select_2(0f,0f,0f,0f,0f,0));
        res1.put(key, new Select_2(
          res1.get(key).sum_qty + l.l_quantity,
          res1.get(key).sum_base_price + l.l_extendedprice,
          res1.get(key).sum_disc_price + (l.l_extendedprice * (1 - l.l_discount)),
          res1.get(key).sum_charge + (l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)),
          res1.get(key).avg_disc + l.l_discount,
          res1.get(key).count_order + 1
        ));
      }
    }

    Set<Map.Entry<Select_1, Select_2>> res2 = res1.entrySet();

    Set<Select_3> res3 = new HashSet<Select_3>();
    for (Map.Entry<Select_1, Select_2> l : res2) {
      res3.add(new Select_3(
          l.getKey().l_returnflag,
          l.getKey().l_linestatus,
          l.getValue().sum_qty,
          l.getValue().sum_base_price,
          l.getValue().sum_disc_price,
          l.getValue().sum_charge,
          l.getValue().sum_qty / l.getValue().count_order,
          l.getValue().sum_base_price / (double) l.getValue().count_order,
          l.getValue().avg_disc / l.getValue().count_order,
          l.getValue().count_order
      ));
    }

    List<Select_3> res4 = new ArrayList<Select_3>(res3);
    java.util.Collections.sort(res4);
    return res4;
  }
}
