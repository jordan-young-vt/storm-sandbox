package helloworld;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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
        //topologyContext.registerMetric("total_count", countMetric, 10);
    }
    @Override
    public void execute(Tuple input) {
        // Get the field "site" from input tuple.

        if (isTickTuple(input)) {
            for (String key: outputMap.keySet()) {
                collector.emit(new Values(key, outputMap.get(key)));
                System.out.println("Total Counts: " + outputMap.toString());
            }
            outputMap.clear();
        }
        else {
            String test = input.getStringByField("site");
            if (outputMap.containsKey(test)) {
                outputMap.put(test, outputMap.get(test) + 1);
            }
            else {
                outputMap.put(test,1);
            }
        }
	    collector.ack(input);
   //     countMetric.incr();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("site", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        return conf;
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}
