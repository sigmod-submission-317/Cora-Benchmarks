package tpch.schema;

import java.io.Serializable;

public class Part  implements Serializable {
  public int p_partkey;
  public String p_name;
  public String p_mfgr;
  public String p_brand;
  public String p_type;
  public int p_size;
  public String p_container;
  public double p_retailprice;
  public String p_comment;
}
