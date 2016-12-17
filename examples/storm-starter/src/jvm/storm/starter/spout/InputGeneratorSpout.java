package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RateTracker;
import backtype.storm.utils.Utils;
import org.apache.commons.collections.map.AbstractHashedMap;
import storm.starter.util.StringGenerator;

import java.util.Map;
import java.util.Random;

/**
 * Created by robert on 1/8/16.
 */
public class InputGeneratorSpout extends BaseRichSpout {

    transient StringGenerator stringGenerator;
    int numberOfDistinctString;
    int warmupTime;
    SpoutOutputCollector outputCollector;
    transient Random random;
    long currentValue;

    RateTracker rateTracker = new RateTracker(10000, 10);

    long startTime;

    public InputGeneratorSpout(int numberOfDistinctString) {
        this(numberOfDistinctString, 0);
    }

    public InputGeneratorSpout(int numberOfDistinctString, int warmupSeconds) {
        this.numberOfDistinctString = numberOfDistinctString;
        warmupTime = warmupSeconds;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "timestamp"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        stringGenerator = new StringGenerator(numberOfDistinctString);
        random = new Random();
        outputCollector = collector;
        currentValue = 0;
        startTime = System.currentTimeMillis();


    }

    @Override
    public void nextTuple() {
        rateTracker.notify(1);
        if(System.currentTimeMillis() - startTime >= warmupTime * 1000)
            Utils.sleep(1);
        outputCollector.emit(new Values(currentValue, System.currentTimeMillis()));
        currentValue = (currentValue + 1) % numberOfDistinctString;
    }
}
