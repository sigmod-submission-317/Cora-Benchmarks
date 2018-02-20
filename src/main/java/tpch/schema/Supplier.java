package tpch.schema;

import java.io.Serializable;

public class Supplier implements Serializable{
  public int s_suppkey;
  public String s_name;
  public String s_address;
  public int s_nationkey;
  public String s_phone;
  public double s_acctbal;
  public String s_comment;
}