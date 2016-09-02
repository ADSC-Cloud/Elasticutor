package storm.starter.bolt;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.WordCountTopologyElastic;
import storm.starter.util.ComputationSimulator;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by robert on 1/8/16.
 */
public class ElasticSimulatorBolt extends BaseElasticBolt{

    long computationCostPerTupleInMicroseconds;

    public ElasticSimulatorBolt(long computationCostPerTupleInMicrosecond) {
        this.computationCostPerTupleInMicroseconds = computationCostPerTupleInMicrosecond;
    }

    @Override
    public void execute(Tuple tuple, ElasticOutputCollector collector) {
//        utils.sleep(sleepTimeInMilics);
        ComputationSimulator.compute(computationCostPerTupleInMicroseconds);
        String word = tuple.getString(0);
        Integer count = (Integer)getValueByKey(word);
        if (count == null)
            count = 0;
        count++;
        setValueByKey(word,count);
        collector.emit(tuple,new Values(word, count));
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }

    @Override
    public Serializable getKey(Tuple tuple) {
        return tuple.getString(0);
    }
}
