package helloworld;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

public class LearningStormGlobalAggBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;

    transient CountMetric countMetric;
    private Map<String,Integer> outputMap = new HashMap<String,Integer>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        //DBDriver.connectionUrl = (String) map.get("connectionURL");
        //DBDriver.dbUsername = (String) map.get("dbUsername");
        //DBDriver.dbPassword = (String) map.get("dbPassword");
    }
    @Override
    public void execute(Tuple input) {
        // Get the field "site" from input tuple.
        Integer count = 0;
        String site = input.getStringByField("site");
        try {
            count = Integer.parseInt(input.getStringByField("count"));
        }
        catch (Exception e) {
        }
        if (outputMap.containsKey(site)) {
            outputMap.put(site, outputMap.get(site) + count);
        }
        else {
            outputMap.put(site,count);
        }
        collector.ack(input);
        System.out.println("Globally Aggregated ßTotal Counts: " + outputMap.toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}