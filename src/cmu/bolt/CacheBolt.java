package cmu;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

public class CacheBolt extends BaseRichBolt {
  OutputCollector _collector;
  transient RedisConnection<String,String> redis;

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;

      //RedisClient client = new RedisClient("localhost",6379);
      //redis = client.connect();
  }

  @Override
  public void execute(Tuple tuple) {

      String country = tuple.getString(0);
      Integer count = tuple.getInteger(1);

      //redis.publish("TweetLocationTopology", country + "|" + Long.toString(count));

      _collector.emit(tuple, new Values(country + " - " + count));
      _collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("hasing"));
  }

}
