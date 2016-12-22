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
import java.util.Random;

/**
 * Created by robert on 22/12/16.
 */
public class ElasticBolt extends BaseElasticBolt {

    private StateConsistencyValidator stateConsistencyValidator;
    private int computationCostInNanaSeconds;

    public ElasticBolt(StateConsistencyValidator stateConsistencyValidator, int computationCostInNanaSeconds) {
        this.stateConsistencyValidator = stateConsistencyValidator;
        this.computationCostInNanaSeconds = computationCostInNanaSeconds;
    }

    @Override
    public Serializable getKey(Tuple tuple) {
        return tuple.getInteger(0);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {
        ComputationSimulator.compute(computationCostInNanaSeconds);
        Integer key = input.getInteger(0);
        Long value = input.getLong(1);
        if(getValueByKey(key) == null) {
            setValueByKey(key, new StateConsistencyValidator.ValidateState());
        }
        StateConsistencyValidator.ValidateState state = (StateConsistencyValidator.ValidateState)getValueByKey(getKey(input));
        if(!stateConsistencyValidator.validate(value, state)) {
            Slave.getInstance().sendMessageToMaster("### Validation fails!");
        }

        setValueByKey(key, state);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }
}
