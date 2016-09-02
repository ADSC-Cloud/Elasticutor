package backtype.storm.elasticity;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.elasticity.state.*;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by Robert on 11/3/15.
 */
public abstract class BaseElasticBolt implements Serializable {

    private transient KeyValueState state;

    public abstract Serializable getKey(Tuple tuple);

    public Serializable getValueByKey(Serializable key) {
        return state.getValueByKey(key);
    }

    public void removeValueByKey(Serializable key) {
        state.removeValueByKey(key);
    }

    public void setValueByKey(Serializable key, Serializable value) {
        state.setValueByKey(key, value);
    }

    public abstract void execute(Tuple input, ElasticOutputCollector collector);

    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

//    public void initialize(Map stormConf, TopologyContext context) {
//        declareStatefulOperator();
//        prepare(stormConf, context);
//    };

    public abstract void prepare(Map stormConf, TopologyContext context);

//    public abstract void prepare(Map stormConf, TopologyContext context);

    public void declareStatefulOperator() {
        state = new KeyValueState();
    }

    public void cleanup() {

    }

    public KeyValueState getState() {
        return state;
    }

    public void setState(KeyValueState s) {
        state = s;
        if(state==null) {
            state = new KeyValueState();
        }
    }
}
