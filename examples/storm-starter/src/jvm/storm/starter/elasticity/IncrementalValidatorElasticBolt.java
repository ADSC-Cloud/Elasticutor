package storm.starter.elasticity;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import storm.starter.elasticity.util.StateConsistencyValidator;
import storm.starter.util.ComputationSimulator;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by robert on 22/12/16.
 */
public class IncrementalValidatorElasticBolt extends BaseElasticBolt {

    private int computationCostInNanaSeconds;
    private long progress = 0;
    private boolean acked;

    public IncrementalValidatorElasticBolt(int computationCostInNanaSeconds, boolean acked) {
        this.computationCostInNanaSeconds = computationCostInNanaSeconds;
        this.acked = acked;
    }

    @Override
    public Serializable getKey(Tuple tuple) {
        return tuple.getInteger(0);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {
        ComputationSimulator.compute(computationCostInNanaSeconds);
        Integer key = input.getInteger(0);
        Long inStateCount = (Long)getValueByKey(key);
        if (inStateCount == null) {
            inStateCount = 0L;
            setValueByKey(key, inStateCount);
        } else {
            Long count = input.getLong(1);
            if(inStateCount + 1 != count) {
                Slave.getInstance().logOnMaster(String.format("Key: %d, expected: %d, actual: %d. Local Progress: %d. ThreadId: %d", key,
                        inStateCount + 1, count, progress++, Thread.currentThread().getId()));
            }
            inStateCount = count;
            setValueByKey(key, inStateCount);
        }
        if (acked)
            collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }
}
