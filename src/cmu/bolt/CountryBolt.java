package cmu;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.Map;

public class CountryBolt extends BaseRichBolt {
  OutputCollector _collector;

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    String location = tuple.getString(0);

    try {
		    String resource = getRemoteJSON(location);
				String country = parseJSON(resource);
        _collector.emit(tuple, new Values(country));
		} catch (Exception e) {
				e.printStackTrace();
    } finally {
      _collector.ack(tuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("country"));
  }

  private String parseJSON(String str) throws JSONException {
    // construct two JSONObject
    // one for source string, one for result string
    JSONObject resource = new JSONObject(str);
    String status = resource.getString("status");
    if(status.equals("OK")) {
      JSONArray results = new JSONArray(resource.getString("results"));
      JSONObject result =  results.getJSONObject(0);
      JSONArray addressComponent = result.getJSONArray("address_components");
      int size = addressComponent.length();
      for(int i = 0; i < size; i++){
        JSONObject country = addressComponent.getJSONObject(i);
        if(country.toString().contains("country"))
          return country.getString("short_name");
        }
    }

    return null;
  }

  private String getRemoteJSON(String location) throws IOException {
  // get connection with service provider
  // note: personal API key is included in the url
      URL url = new URL("http://maps.googleapis.com/maps/api/geocode/json?latlng="
                    + location);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setDoOutput(true);
      connection.setDoInput(true);
      // set request method
      connection.setRequestMethod("GET");
      connection.setUseCaches(false);
      // set the format of response body
      connection.connect();
      // get response from buffered reader
      BufferedReader reader = new BufferedReader(new InputStreamReader(
              connection.getInputStream()));
      // read response line by line
      String line = null;
      StringBuffer response = new StringBuffer();
      while ((line = reader.readLine()) != null) {
          response.append(line);
      }
      reader.close();
      connection.disconnect();

      return response.toString();
  }
}
