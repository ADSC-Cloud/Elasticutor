package storm.starter.elasticity;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.elasticity.util.StateConsistencyValidator;

import java.util.Map;

/**
 * Created by robert on 22/12/16.
 */
public class XORValidatorSpout extends BaseRichSpout {

    private StateConsistencyValidator validator;
    private SpoutOutputCollector collector;
    private StateConsistencyValidator.Generator generator;

    public XORValidatorSpout(StateConsistencyValidator validator) {
        this.validator = validator;
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.generator = validator.createGenerator();
    }

    @Override
    public void nextTuple() {
//        Utils.sleep(1000);
        StateConsistencyValidator.ValidateTuple validateTuple = generator.generate();
        collector.emit(new Values(validateTuple.key, validateTuple.value), new Object());
//        Slave.getInstance().sendMessageToMaster("Emitted: " + validateTuple.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }
}
