package tpch.seq;

import tpch.schema.LineItem;
import tpch.schema.Part;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Query17 {
  static class Select_1 implements Serializable {
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

  public static double run (Set<LineItem> lineitem, Set<Part> part) {
    String brand = "Brand#23";
    String container = "MED BOX";

    Set<Integer> res0 = new HashSet<Integer>();
    for (Part p : part) {
      if (p.p_brand.equals(brand) && p.p_container.equals(container))
        res0.add(p.p_partkey);
    }

    Map<Integer, Select_1> res1 = new HashMap<Integer, Select_1>();
    for (LineItem l : lineitem) {
      if (!res1.containsKey(l.l_partkey))
        res1.put(l.l_partkey, new Select_1(0, 0));
      res1.put(l.l_partkey, new Select_1(res1.get(l.l_partkey).sum + l.l_quantity, res1.get(l.l_partkey).count + 1));
    }

    Set<Map.Entry<Integer, Select_1>> res2 = res1.entrySet();

    Set<Select_2> res3 = new HashSet<Select_2>();
    for (Map.Entry<Integer, Select_1> p : res2) {
        res3.add(new Select_2(p.getKey(), 0.2f * (p.getValue().sum / p.getValue().count)));
    }

    Set<Select_2> res4 = new HashSet<Select_2>();
    for (Integer pk : res0) {
      for (Select_2 a : res3) {
        if (pk == a.p_partkey)
          res4.add(a);
      }
    }

    Set<Select_3> res5 = new HashSet<Select_3>();
    for (LineItem l : lineitem) {
      for(Select_2 avg : res4){
        if (l.l_partkey == avg.p_partkey)
          res5.add(new Select_3(l.l_quantity, avg.avg,l.l_extendedprice));
      }
    }

    double avg_yearly = 0;
    for (Select_3 r : res5){
      if (r.quantity < r.average)
        avg_yearly += r.extendedprice;
    }

    avg_yearly = avg_yearly / 7.0f;
    return avg_yearly;
  }
}