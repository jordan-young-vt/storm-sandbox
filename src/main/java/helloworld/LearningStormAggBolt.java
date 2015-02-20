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

public class LearningStormAggBolt extends BaseRichBolt {

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
        topologyContext.registerMetric("total_count", countMetric, 10);
    }

    public void execute(Tuple input) {
        // Get the field "site" from input tuple.
        String test = input.getStringByField("site");
        if (outputMap.containsKey(test)) {
            outputMap.put(test, outputMap.get(test) + 1);
        }
        else {
            outputMap.put(test,1);
        }

        countMetric.incr();
        collector.ack(input);
        System.out.println("Total Counts: " + outputMap.toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
