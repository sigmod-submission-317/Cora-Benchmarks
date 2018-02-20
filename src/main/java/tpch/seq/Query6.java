package tpch.seq;

import tpch.schema.LineItem;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Query6 {
  public static double run (List<LineItem> lineitem) throws ParseException {
    Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse("1994-01-01");
    Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse("1995-01-01");
    double revenue = 0;
    for (LineItem l : lineitem) {
      if (
          !l.l_shipdate.before(d1) &&
          l.l_shipdate.before(d2) &&
          l.l_discount >= 0.05 &&
          l.l_discount <= 0.07 &&
          l.l_quantity < 25
      )
        revenue += (l.l_extendedprice * l.l_discount);
    }
    return revenue;
  }
}