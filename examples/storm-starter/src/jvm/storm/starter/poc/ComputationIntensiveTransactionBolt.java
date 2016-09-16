package storm.starter.poc;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.state.KeyValueState;
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

    List<Integer> upstreamTaskIds;

    private int taskId;

    int receivedMigrationCommand;

    public static class State implements Serializable{

        public List<Record> buys;
        public List<Record> sells;

        public State() {
            sells = Collections.synchronizedList(new ArrayList<Record>());
            buys = Collections.synchronizedList(new ArrayList<Record>());
        }

        public List<Record> getSells() {
            List<Record> list = new ArrayList<>(sells);
            Collections.sort(list, Record.getPriceComparator());
            return list;
//            return sells;
        }

        public List<Record> getBuys() {
            List<Record> list = new ArrayList<>(buys);
            Collections.sort(list, Record.getPriceReverseComparator());
            return list;
//            return buys;
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

//        public void updateSell(Record record) {
//            sells.add(record);
//        }
//
//        public void updateBuy(Record record) {
//            buys.add(record);
//        }

        public void removeBuy(Record record) {
            buys.remove(record);
        }
        public void removeSell(Record record) {
            sells.remove(record);
        }
    }
    @Override
    public Serializable getKey(Tuple tuple) {
        try {
            return tuple.getIntegerByField(PocTopology.SEC_CODE);
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {

        final String streamId = input.getSourceStreamId();

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
            List<Record> sells = state.getSells();

            for(Record sell: sells) {
                if(newRecord.volume == 0) {
                    break;
                }
                double tradeVolume = Math.min(newRecord.volume, sell.volume);
                newRecord.volume -= tradeVolume;
                sell.volume -= tradeVolume;
//                state.updateSell(sell);
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
            if(new Random().nextDouble()<0.1) {
                long startTime = input.getLongByField(PocTopology.EMIT_TIME_STAMP);
                collector.emit(PocTopology.LATENCY_REPORT_STREAM, new Values(System.currentTimeMillis() - startTime));
            }
            collector.ack(input);

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
            List<Record> buys = state.getBuys();

            for(Record buy: buys) {
                if(newRecord.volume == 0) {
                    break;
                }
                double tradeVolume = Math.min(newRecord.volume, buy.volume);
                newRecord.volume -= tradeVolume;
                buy.volume -= tradeVolume;
//                state.updateBuy(buy);
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
            if(new Random().nextDouble()<0.1) {
                long startTime = input.getLongByField(PocTopology.EMIT_TIME_STAMP);
                collector.emit(PocTopology.LATENCY_REPORT_STREAM, new Values(System.currentTimeMillis() - startTime));
            }
            collector.ack(input);
        } else if (streamId.equals(PocTopology.STATE_MIGRATION_COMMAND_STREAM)) {
            receivedMigrationCommand++;
            System.out.println("receivedMigrationCommand: " + receivedMigrationCommand);
            if(receivedMigrationCommand == upstreamTaskIds.size()) {
                int sourceTaskOffset = input.getInteger(0);
                int targetTaskOffset = input.getInteger(1);
                int shardId = input.getInteger(2);
//                Slave.getInstance().logOnMaster(String.format("Task %d Received StateMigrationCommand %d: %d--->%d.", taskId, shardId, sourceTaskOffset, targetTaskOffset));

                // received the migration command from each of the upstream tasks.
                receivedMigrationCommand = 0;
                KeyValueState state = getState();

//                state.getState().put("key", new byte[1024 * 32]);

                Slave.getInstance().logOnMaster("State migration starts!");
                System.out.println("State migration starts!");
                collector.emit(PocTopology.STATE_MIGRATION_STREAM, new Values(sourceTaskOffset, targetTaskOffset, shardId, state));
            }
        } else if (streamId.equals(PocTopology.STATE_UPDATE_STREAM)) {
            System.out.println("Received state update stream!");
            int targetTaskOffset = input.getInteger(0);
            KeyValueState state = (KeyValueState) input.getValue(1);
            getState().update(state);
            Slave.getInstance().logOnMaster("State is updated!");
            System.out.println("State is updated!");
            collector.emit(PocTopology.STATE_READY_STREAM, input, new Values(targetTaskOffset));

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(PocTopology.TRANSACTION_STREAM, new Fields(PocTopology.SEC_CODE, PocTopology.PRICE, PocTopology.VOLUME));
        declarer.declareStream(PocTopology.LATENCY_REPORT_STREAM, new Fields(PocTopology.EMIT_TIME_STAMP));
        declarer.declareStream(PocTopology.STATE_MIGRATION_STREAM, new Fields("sourceTaskId", "targetTaskId", "shardId", "state"));
        declarer.declareStream(PocTopology.STATE_READY_STREAM, new Fields("targetTaskId"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
        upstreamTaskIds = context.getComponentTasks(PocTopology.ForwardBolt);
        taskId = context.getThisTaskId();
        receivedMigrationCommand = 0;
    }
}
