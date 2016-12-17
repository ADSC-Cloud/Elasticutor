package storm.starter.poc;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.state.KeyValueState;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.ResourceCentricZipfComputationTopology;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by robert on 25/5/16.
 */
public class TransactionBolt extends BaseElasticBolt{


    List<Integer> upstreamTaskIds;

    private int taskId;

    int receivedMigrationCommand;


    enum direction{buy, sell};

    public static class State implements Serializable{

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
    public Serializable getKey(Tuple tuple) {
        return tuple.getIntegerByField(PocTopology.SEC_CODE);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {

        String streamId = input.getSourceStreamId();

        if(streamId.equals(PocTopology.BUYER_STREAM)) {
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

        } else if (streamId.equals(PocTopology.SELLER_STREAM)){
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
        } else if (streamId.equals(PocTopology.STATE_MIGRATION_COMMAND_STREAM)) {
            receivedMigrationCommand++;
            if(receivedMigrationCommand==upstreamTaskIds.size()) {
                int sourceTaskOffset = input.getInteger(0);
                int targetTaskOffset = input.getInteger(1);
                int shardId = input.getInteger(2);
//                Slave.getInstance().logOnMaster(String.format("Task %d Received StateMigrationCommand %d: %d--->%d.", executorId, shardId, sourceTaskOffset, targetTaskOffset));

                // received the migration command from each of the upstream tasks.
                receivedMigrationCommand = 0;
                KeyValueState state = getState();

//                state.getState().put("key", new byte[1024 * 32]);

                Slave.getInstance().logOnMaster("State migration starts!");
                collector.emit(PocTopology.STATE_MIGRATION_STREAM, new Values(sourceTaskOffset, targetTaskOffset, shardId, state));
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();

        upstreamTaskIds = context.getComponentTasks(PocTopology.ForwardBolt);
        taskId = context.getThisTaskId();
        receivedMigrationCommand = 0;

    }
}
