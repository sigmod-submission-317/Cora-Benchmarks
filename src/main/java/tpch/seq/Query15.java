package tpch.seq;

import scala.Serializable;
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

  public static List<Select_1> run (Set<LineItem> lineitem, Set<Supplier> supplier) throws ParseException {
    Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("1996-04-01");
    Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse("1996-01-01");

    Map<Integer, Double> res0 = new HashMap<Integer, Double>();
    for (LineItem l : lineitem) {
      if (!l.l_shipdate.before(d2) && l.l_shipdate.before(d1)){
        if (!res0.containsKey(l.l_suppkey))
          res0.put(l.l_suppkey, 0.0);
        res0.put(l.l_suppkey, res0.get(l.l_suppkey) + (l.l_extendedprice * (1 - l.l_discount)));
      }
    }

    Set<Map.Entry<Integer, Double>> revenue0 = res0.entrySet();

    double max_total = Double.MIN_VALUE;
    for (Map.Entry<Integer, Double> e : revenue0){
      if (max_total < e.getValue()) max_total = e.getValue();
    }

    Set<Map.Entry<Integer, Double>> res1 = new HashSet<Map.Entry<Integer, Double>>();
    for (Map.Entry<Integer, Double> e : revenue0){
      if (e.getValue() == max_total)
        res1.add(e);
    }

    Set<Select_1> res2 = new HashSet<Select_1>();
    for (Map.Entry<Integer, Double> e : revenue0){
      for(Supplier s : supplier){
        if (s.s_suppkey == e.getKey())
          res2.add(new Select_1(s.s_suppkey, s.s_name, s.s_address, s.s_phone, e.getValue()));
      }
    }

    List<Select_1> res3 = new ArrayList<Select_1>(res2);
    java.util.Collections.sort(res3);
    return res3;
  }
}