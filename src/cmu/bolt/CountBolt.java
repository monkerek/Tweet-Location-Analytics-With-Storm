package cmu;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {
  OutputCollector _collector;
  Map<String, Integer> countMap;

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      countMap = new HashMap<String, Integer>();
  }

  @Override
  public void execute(Tuple tuple) {
      String country = tuple.getString(0);

      if (countMap.get(country) == null) {
        countMap.put(country, 1);
      }
      else {
        Integer val = countMap.get(country);
        countMap.put(country, val + 1);
      }

      _collector.emit(tuple, new Values(country, countMap.get(country)));
      _collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("country", "count"));
  }

}
