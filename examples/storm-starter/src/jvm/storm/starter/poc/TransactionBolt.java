package storm.starter.poc;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by robert on 25/5/16.
 */
public class TransactionBolt extends BaseElasticBolt{
    enum direction{buy, sell};

    public static class State{

        public TreeMap<Double, List<Record>> sells;
        public TreeMap<Double, List<Record>> buys;

        public State() {
            sells = new TreeMap<>();
            buys = new TreeMap<>();
        }

        public Map.Entry<Double, List<Record>> getBestSells() {
            return sells.firstEntry();
        }

        public Map.Entry<Double, List<Record>> getBestBuys() {
            return buys.lastEntry();
        }

        public void insertBuy(Record record) {
            Double price = record.price;
            if(!buys.containsKey(price)) {
                buys.put(price, new ArrayList<Record>());
            }
            buys.get(price).add(record);
        }

        public void insertSell(Record record) {
            Double price = record.price;
            if(!sells.containsKey(price)) {
                sells.put(price, new ArrayList<Record>());
            }
            sells.get(price).add(record);
        }
    }
    @Override
    public Object getKey(Tuple tuple) {
        return tuple.getIntegerByField(PocTopology.SEC_CODE);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {

        State state = (State)getValueByKey(getKey(input));
        if(state == null) {
            state = new State();
            setValueByKey(getKey(input), state);
        }

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");
        Date date = null;
        try {
            date = format.parse(String.format("%s %s.%d", input.getStringByField(PocTopology.DATE), input.getStringByField(PocTopology.TIME), (int)(1000 * Double.parseDouble(input.getStringByField(PocTopology.MILLISECOND)))));
        } catch (Exception e) {
            e.printStackTrace();
            Slave.getInstance().sendMessageToMaster(e.getMessage());
            return;
        }
        Record newRecord = new Record(
                input.getLongByField(PocTopology.ORDER_NO),
                input.getStringByField(PocTopology.ACCT_ID),
                input.getDoubleByField(PocTopology.PRICE),
                input.getIntegerByField(PocTopology.VOLUME),
                input.getIntegerByField(PocTopology.SEC_CODE),
                date);

        if(input.getSourceStreamId().equals(PocTopology.BUYER_STREAM)) {

            while(state.getBestSells()!=null && newRecord.volume > 0) {
                Map.Entry<Double, List<Record>> entry = state.getBestSells();
                Double sellPrice = entry.getKey();
                List<Record> records = entry.getValue();
                if(sellPrice <= newRecord.price) {
                    while(newRecord.volume > 0 && !records.isEmpty()) {
                        Record sell = records.get(0);
                        double tradeVolume = Math.min(newRecord.volume, sell.volume);
                        newRecord.volume -= tradeVolume;
                        sell.volume -= tradeVolume;
                        System.out.println(String.format("User %s buy %f volume %s stock from User %s at price %.2f", newRecord.accountId, tradeVolume, newRecord.secCode, sell.accountId, sellPrice));
                        if(sell.volume == 0) {
                            records.remove(0);
                            System.out.println(String.format("Seller %s's transaction for stock %d! is completed!", sell.accountId, sell.secCode));
                        }
                    }
                    if(records.isEmpty()) {
                        state.sells.remove(sellPrice);
                    }
                } else {
                    break;
                }

            }
            if(newRecord.volume > 0) {
                state.insertBuy(newRecord);
            }

        } else {

            while(state.getBestBuys()!=null && newRecord.volume > 0) {
                Map.Entry<Double, List<Record>> entry = state.getBestBuys();
                Double BuyPrice = entry.getKey();
                List<Record> records = entry.getValue();
                if(BuyPrice >= newRecord.price) {
                    while(newRecord.volume > 0 && !records.isEmpty()) {
                        Record buy = records.get(0);
                        double tradeVolume = Math.min(newRecord.volume, buy.volume);
                        newRecord.volume -= tradeVolume;
                        buy.volume -= tradeVolume;
                        System.out.println(String.format("User %s buy %f volume %s stock from User %s at price %.2f", buy.accountId, tradeVolume, buy.secCode, newRecord.accountId, BuyPrice));
                        if(buy.volume == 0) {
                            records.remove(0);
                            System.out.println(String.format("Buyer %s's transaction for stock %d! is completed!", buy.accountId, buy.secCode));
                        }
                    }
                    if(records.isEmpty()) {
                        state.buys.remove(BuyPrice);
                    }
                } else {
                    break;
                }

            }
            if(newRecord.volume > 0) {
                state.insertSell(newRecord);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }
}
