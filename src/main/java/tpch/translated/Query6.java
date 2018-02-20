package tpch.translated;

import org.apache.spark.api.java.JavaRDD;
import tpch.schema.LineItem;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Query6 {
  public static double run (JavaRDD<LineItem> lineitem) throws ParseException {
    Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("1994-01-01");
    Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse("1995-01-01");
    double revenue = 0;
    revenue = 0;
    revenue = lineitem.flatMap(lineitem_index -> {
      List<Double> emits = new ArrayList<Double>();
      if (((((((lineitem_index.l_discount >= 0.05) && (lineitem_index.l_quantity < 24)) && (!lineitem_index.l_shipdate.before(d1))) && lineitem_index.l_shipdate.before(d2)) && (lineitem_index.l_quantity <= lineitem_index.l_quantity)) && (lineitem_index.l_discount <= 0.07))) emits.add(lineitem_index.l_extendedprice * lineitem_index.l_discount);
      return emits.iterator();
    }).reduce((val1, val2) -> val1 + val2);
    return revenue;
  }
}