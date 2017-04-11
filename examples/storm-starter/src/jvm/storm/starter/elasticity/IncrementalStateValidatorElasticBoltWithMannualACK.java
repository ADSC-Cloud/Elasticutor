package storm.starter.elasticity;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.elasticity.util.StateConsistencyValidator;
import storm.starter.util.ComputationSimulator;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by robert on 22/12/16.
 */
public class IncrementalStateValidatorElasticBoltWithMannualACK extends BaseElasticBolt {

    private int computationCostInNanaSeconds;
    private long progress = 0;
    private boolean acked;

    public IncrementalStateValidatorElasticBoltWithMannualACK(int computationCostInNanaSeconds, boolean acked) {
        this.computationCostInNanaSeconds = computationCostInNanaSeconds;
        this.acked = acked;
    }

    @Override
    public Serializable getKey(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("backpressure"))
            return 0;
        return tuple.getInteger(0);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {
        if (input.getSourceStreamId().equals("backpressure")) {
            collector.emitDirect(input.getInteger(1), "backpressure", new Values(input.getLong(0)));
            return;
        }
        Integer key = input.getInteger(0);
        ComputationSimulator.sleep(computationCostInNanaSeconds, key);
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
        declarer.declareStream("backpressure", new Fields("tupleId"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }
}
