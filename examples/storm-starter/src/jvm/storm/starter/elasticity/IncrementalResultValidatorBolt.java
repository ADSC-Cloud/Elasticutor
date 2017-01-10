package storm.starter.elasticity;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 10/1/17.
 */
public class IncrementalResultValidatorBolt implements IRichBolt {

    Map<Integer, Long> keyToCount;
    int resultFrequency;

    public IncrementalResultValidatorBolt(int resultFrequency) {
        this.resultFrequency = resultFrequency;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        keyToCount = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        Integer key = input.getInteger(0);
        Long count = input.getLong(1);
        if (! keyToCount.containsKey(key)) {
            keyToCount.put(key, count);
        } else {
            if (keyToCount.get(key) + resultFrequency != count) {
                Slave.getInstance().logOnMaster(String.format("Key %d 's result expected: %d, received: %d!", key,
                        keyToCount.get(key) + resultFrequency, count));
            }
            keyToCount.put(key, count);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
