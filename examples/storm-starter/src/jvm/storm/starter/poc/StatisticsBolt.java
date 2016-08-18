package storm.starter.poc;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by robert on 16-8-18.
 */
public class StatisticsBolt extends BaseElasticBolt {

    class TransactionRecord implements Serializable {
        TransactionRecord(double price, double volume) {
            this.price = price;
            this.volume = volume;
        }
        double price;
        double volume;
    }

    class TransactionHistory implements Serializable {
        ConcurrentLinkedQueue<TransactionRecord> transactions = new ConcurrentLinkedQueue<>();
        public void insertNewRecord(double price, double volume) {
            transactions.add(new TransactionRecord(price, volume));
        }

        public double getAveragePrice() {
            double price = 0;
            double totalVolume = 0;
            for(TransactionRecord record: transactions) {
                price += record.volume * record.price;
                totalVolume += record.volume;
            }
            return price / totalVolume;
        }

        public double getTotalVolume() {
            double volume = 0;
            for(TransactionRecord record: transactions) {
                volume += record.volume;
            }
            return volume;
        }

        public double getVariance() {
            double diff = 0;
            double avg = getAveragePrice();
            double totalVolume = getTotalVolume();
            for(TransactionRecord record: transactions) {
                diff += Math.pow((record.price), 2) * record.volume / totalVolume;
            }
            return diff - Math.pow(avg, 2);
        }
    }

    @Override
    public Object getKey(Tuple tuple) {
        return tuple.getInteger(0);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {
        TransactionHistory history = (TransactionHistory) getValueByKey(getKey(input));
        if(history == null) {
            history = new TransactionHistory();
            setValueByKey(getKey(input), history);
        }

        final double price = input.getDouble(1);
        final double volume = input.getDouble(2);
        history.insertNewRecord(price, volume);
        history.getAveragePrice();
        history.getVariance();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }
}
