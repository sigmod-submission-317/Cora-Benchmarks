package pagerank;

import java.io.Serializable;

public class Link  implements Serializable {
  public String src_url;
  public String dst_url;
  public Link(String s, String d){
    src_url = s;
    dst_url = d;
  }
}