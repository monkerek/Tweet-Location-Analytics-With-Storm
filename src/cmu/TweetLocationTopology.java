package cmu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import cmu.spout.*;
import cmu.bolt.*;


public class TweetLocationTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    TwitterSampleSpout twitterSampleSpout = new TwitterSampleSpout(
        "ZPEsxdhAG1NJ2LmqVDmCPggyn",
        "TcyVIXZLoACiMDfk247DKxFhlywe7ji4mungHYWwnIbR3OSq2j",
        "347235729-TJhOSiEX3p2VyPWPsZm5l0fE3sKE4UwZCt8aVioQ",
        "R4vRq95WY41PSMmRFajRhRdAa60V4OGPDjY7wtE7JjvY4",
        null
    );

    builder.setSpout("tweets", twitterSampleSpout, 10);
    builder.setBolt("country", new CountryBolt(), 10).shuffleGrouping("tweets");
    builder.setBolt("count", new CountBolt(), 10).fieldsGrouping("country", new Fields("country"));
    builder.setBolt("cache", new CacheBolt(), 10).shuffleGrouping("count");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("tweet_location", conf, builder.createTopology());
      Utils.sleep(100000);
      cluster.killTopology("tweet_location");
      cluster.shutdown();
    }
  }
}
