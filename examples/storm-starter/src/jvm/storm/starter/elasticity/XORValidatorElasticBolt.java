package storm.starter.elasticity;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import storm.starter.elasticity.util.StateConsistencyValidator;
import storm.starter.util.ComputationSimulator;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 22/12/16.
 */
public class XORValidatorElasticBolt extends BaseElasticBolt {

    private StateConsistencyValidator stateConsistencyValidator;
    private int computationCostInNanaSeconds;

    public XORValidatorElasticBolt(StateConsistencyValidator stateConsistencyValidator, int computationCostInNanaSeconds) {
        this.stateConsistencyValidator = stateConsistencyValidator;
        this.computationCostInNanaSeconds = computationCostInNanaSeconds;
    }

    @Override
    public Serializable getKey(Tuple tuple) {
        return tuple.getInteger(0);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {
//        Utils.sleep(1000);
        ComputationSimulator.compute(computationCostInNanaSeconds);
        Integer key = input.getInteger(0);
        Long value = input.getLong(1);
//        Slave.getInstance().sendMessageToMaster(String.format("Received: key: %d value: %d", key, value));
        StateConsistencyValidator.ValidateState state = (StateConsistencyValidator.ValidateState)getValueByKey(key);
        if(state == null) {
            state = new StateConsistencyValidator.ValidateState();
            setValueByKey(key, state);
        }
        if(!stateConsistencyValidator.validate(value, state)) {
            Slave.getInstance().sendMessageToMaster("### Validation fails!");
        }
        setValueByKey(key, state);
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
