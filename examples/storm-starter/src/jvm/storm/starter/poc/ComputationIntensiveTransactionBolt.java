package storm.starter.poc;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by robert on 25/5/16.
 */
public class ComputationIntensiveTransactionBolt extends BaseElasticBolt{

    public static class State implements Serializable{

        public List<Record> buys;
        public List<Record> sells;

        public State() {
            sells = new ArrayList<>();
            buys = new ArrayList<>();
        }

        public List<Record> getSells() {
            List<Record> list = new ArrayList<>(sells);
            Collections.sort(list, Record.getPriceComparator());
            return list;
        }

        public List<Record> getBuys() {
            List<Record> list = new ArrayList<>(buys);
            Collections.sort(list, Record.getPriceReverseComparator());
            return list;
        }


        final int maxHistory = 1000;

        public void insertBuy(Record record) {
            if(buys.size() >= maxHistory) {
                buys.remove(0);
            }
            buys.add(record);
        }

        public void insertSell(Record record) {
            if(sells.size() >= maxHistory) {
                sells.remove(0);
            }
            sells.add(record);
        }

        public void updateSell(Record record) {
            sells.add(record);
        }

        public void updateBuy(Record record) {
            buys.add(record);
        }

        public void removeBuy(Record record) {
            buys.remove(record);
        }
        public void removeSell(Record record) {
            sells.remove(record);
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

            List<Record> sells = state.getSells();

            for(Record sell: sells) {
                if(newRecord.volume == 0) {
                    break;
                }
                double tradeVolume = Math.min(newRecord.volume, sell.volume);
                newRecord.volume -= tradeVolume;
                sell.volume -= tradeVolume;
                state.updateSell(sell);
                collector.emit(PocTopology.TRANSACTION_STREAM, new Values(input.getIntegerByField(PocTopology.SEC_CODE), newRecord.price, tradeVolume));
//                System.out.println(String.format("User %s buys %f volume %s stock from User %s at price %.2f on %s.", newRecord.accountId, tradeVolume, newRecord.secCode, sell.accountId, sell.price, format.format(newRecord.date)));
                if(sell.volume == 0) {
                    state.removeSell(sell);
//                    System.out.println(String.format("Seller %s's transaction for stock %d! is completed!", sell.accountId, sell.secCode));
                }
            }
            if(newRecord.volume > 0) {
                state.insertBuy(newRecord);
            }

        } else {
            List<Record> buys = state.getBuys();

            for(Record buy: buys) {
                if(newRecord.volume == 0) {
                    break;
                }
                double tradeVolume = Math.min(newRecord.volume, buy.volume);
                newRecord.volume -= tradeVolume;
                buy.volume -= tradeVolume;
                state.updateBuy(buy);
                collector.emit(PocTopology.TRANSACTION_STREAM, new Values(input.getIntegerByField(PocTopology.SEC_CODE), newRecord.price, tradeVolume));
//                System.out.println(String.format("User %s sells %f volume %s stock to User %s price %.2f on %s.", newRecord.accountId, tradeVolume, newRecord.secCode, buy.accountId, buy.price, format.format(newRecord.date)));
                if(buy.volume == 0) {
                    state.removeBuy(buy);
//                    System.out.println(String.format("Buyer %s's transaction for stock %d! is completed!", buy.accountId, buy.secCode));
                }
            }
            if(newRecord.volume > 0) {
                state.insertSell(newRecord);
            }
        }
        collector.ack(input);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(PocTopology.TRANSACTION_STREAM, new Fields(PocTopology.SEC_CODE, PocTopology.PRICE, PocTopology.VOLUME));

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }
}