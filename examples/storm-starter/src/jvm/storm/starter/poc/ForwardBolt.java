package storm.starter.poc;

import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.google.common.util.concurrent.Runnables;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by robert on 13/9/16.
 */
public class ForwardBolt extends BaseRichBolt {

    OutputCollector collector;
    int taskId;
    int taskIndex;
    List<Integer> downStreamTaskIds;
    int numberOfTransactionTasks;

    transient BalancedHashRouting routingTable;

    BlockingQueue<Tuple> tuples;

    Emitter emitter;

    class Emitter implements Runnable {

        boolean terminating = false;
        boolean terminated = false;

        public void terminate() {
            terminating = true;
            while(!terminated) {
                Utils.sleep(1);
            }
        }

        @Override
        public void run() {
            try {
                while(!terminating) {
                    Tuple tuple = null;
                    try {
                        tuple = tuples.poll(10, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        if(terminating)
                            throw new InterruptedException();
                    }
                    if(tuple == null)
                        continue;
                    final String sourceStream = tuple.getSourceStreamId();
                    final int secCode = tuple.getIntegerByField(PocTopology.SEC_CODE);
                    final int targetTaskIndex = routingTable.route(secCode);
                    final int targetTaskId = downStreamTaskIds.get(targetTaskIndex);
                    collector.emitDirect(targetTaskId, sourceStream, tuple.getValues());
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            terminated = true;
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        tuples = new LinkedBlockingQueue<>();

        this.collector = collector;
        this.taskId = context.getThisTaskId();

        taskIndex = -1;
        for(int i = 0; i < context.getComponentTasks(PocTopology.ForwardBolt).size(); i++) {
            if(taskId == context.getComponentTasks(PocTopology.ForwardBolt).get(i)) {
                taskIndex = i;
            }
        }

        downStreamTaskIds = context.getComponentTasks(PocTopology.TransactionBolt);
        numberOfTransactionTasks = downStreamTaskIds.size();

        routingTable = new BalancedHashRouting(numberOfTransactionTasks);

        emitter = new Emitter();
        new Thread(emitter).start();


    }

    @Override
    public void execute(Tuple input) {
        String sourceStream = input.getSourceStreamId();
        if(sourceStream.equals(PocTopology.BUYER_STREAM) || sourceStream.equals(PocTopology.SELLER_STREAM)) {
//            final int secCode = input.getIntegerByField(PocTopology.SEC_CODE);
//            final int targetTaskIndex = routingTable.route(secCode);
//            final int targetTaskId = downStreamTaskIds.get(targetTaskIndex);
//            collector.emitDirect(targetTaskId, sourceStream, input.getValues());
            tuples.add(input);
        } else if(sourceStream.equals(PocTopology.UPSTREAM_COMMAND_STREAM)) {
            final String command = input.getString(0);
            if(command.equals("getHistograms")) {
                collector.emit(PocTopology.STATISTICS_STREAM, new Values(taskId, routingTable.getBucketsDistribution()));
            } else if(command.equals("pausing")) {
                final int sourceTaskOffset = input.getInteger(1);
                final int targetTaskOffset = input.getInteger(2);
                final int shardId = input.getInteger(3);
                System.out.println("Begin to terminate emit thread..");
                emitter.terminate();
                System.out.println("Terminated!");

                routingTable.reassignBucketToRoute(shardId, targetTaskOffset);
                collector.emitDirect(downStreamTaskIds.get(sourceTaskOffset), PocTopology.STATE_MIGRATION_COMMAND_STREAM, new Values(sourceTaskOffset, targetTaskOffset, shardId));
            } else if (command.equals("resuming")) {
                int sourceTaskIndex = input.getInteger(1);
                emitter = new Emitter();
                new Thread(emitter).start();
                collector.emit(PocTopology.FEEDBACK_STREAM, new Values("resumed", sourceTaskIndex));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(PocTopology.BUYER_STREAM, new Fields(PocTopology.ORDER_NO, PocTopology.ACCT_ID, PocTopology.SEC_CODE, PocTopology.PRICE, PocTopology.VOLUME, PocTopology.DATE, PocTopology.TIME, PocTopology.MILLISECOND, PocTopology.EMIT_TIME_STAMP));
        declarer.declareStream(PocTopology.SELLER_STREAM, new Fields(PocTopology.ORDER_NO, PocTopology.ACCT_ID, PocTopology.SEC_CODE, PocTopology.PRICE, PocTopology.VOLUME, PocTopology.DATE, PocTopology.TIME, PocTopology.MILLISECOND, PocTopology.EMIT_TIME_STAMP));
        declarer.declareStream(PocTopology.STATISTICS_STREAM, new Fields("taskId", "Histograms"));
        declarer.declareStream(PocTopology.STATE_MIGRATION_COMMAND_STREAM, new Fields("sourceTaskId","targetTaskId", "shardId"));
        declarer.declareStream(PocTopology.FEEDBACK_STREAM, new Fields("command", "arg1"));

    }
}
