package pagerank.seq;

import pagerank.Link;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class PageRank {
  public static Map<String, Double> run(List<Link> links){
    HashMap<String, Double> ranks = new HashMap<>();
    HashMap<String, List<String>> grouped_links = new HashMap<>();

    for (Link link : links) {
      if (!grouped_links.containsKey(link.src_url))
        grouped_links.put(link.src_url, new ArrayList<>());
      grouped_links.get(link.src_url).add(link.dst_url);
    }

    for (String link : grouped_links.keySet()) {
      ranks.put(link, 1.0);
    }

    for (int i=0; i<10; i++) {
      HashMap<String, Double> contrib = new HashMap<>();
      for (Map.Entry<String, Double> r : ranks.entrySet()) {
        List<String> urls = grouped_links.get(r.getKey());
        int size = urls.size();
        urls.forEach(dst -> {
          if(!contrib.containsKey(dst))
            contrib.put(dst, 0.0);
          contrib.put(dst, contrib.get(dst) + (r.getValue() / size));
        });
      }

      for (String dst : contrib.keySet()) {
        ranks.put(dst, contrib.get(dst) * 0.85 + 0.15);
      }
    }

    return ranks;
  }
}