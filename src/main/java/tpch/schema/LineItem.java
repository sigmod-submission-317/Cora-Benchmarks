package tpch.schema;

import java.io.Serializable;
import java.util.Date;

public class LineItem implements Serializable{
  public int l_orderkey;
  public int l_partkey;
  public int l_suppkey;
  public int l_linenubmer;
  public double l_quantity;
  public double l_extendedprice;
  public double l_discount;
  public double l_tax;
  public char l_returnflag;
  public char l_linestatus;
  public Date l_shipdate;
  public Date l_commitdate;
  public Date l_receiptdate;
  public String l_shipinstruct;
  public String l_shipmode;
  public String l_comment;
}