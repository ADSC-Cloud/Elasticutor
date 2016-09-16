package storm.starter.poc;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by robert on 25/5/16.
 */
public class PocTopology {
    static String SEC_CODE = "sec_code";
    static String ACCT_ID = "acct_id";
    static String ORDER_NO = "order_no";
    static String PRICE = "price";
    static String VOLUME = "volume";
    static String DATE = "date";
    static String TIME = "time";
    static String MILLISECOND = "millisecond";
    static String EMIT_TIME_STAMP = "emit_time_stamp";


    static String BUYER_STREAM = "buyer_stream";
    static String SELLER_STREAM = "seller_stream";
    static String TRANSACTION_STREAM = "transaction_stream";
    static String LATENCY_REPORT_STREAM = "latency_report_stream";
    static String UPSTREAM_COMMAND_STREAM = "upstream_command_stream";
    static String STATISTICS_STREAM = "statistics_stream";
    static String STATE_MIGRATION_COMMAND_STREAM = "state_migration_command_stream";
    static String STATE_MIGRATION_STREAM = "state_migration_stream";
    static String STATE_UPDATE_STREAM = "state_update_stream";
    static String STATE_READY_STREAM = "state_ready_stream";
    static String FEEDBACK_STREAM = "feedback_stream";


    static String Spout = "spout";
    static String TransactionBolt = "TransactionBolt";
    static String StatisticsBolt = "StatisticsBolt";
    static String LatencyReportBolt = "LatencyReportBolt";
    static String ForwardBolt = "ForwardBolt";
    static String ControllerBolt = "ControllerBolt";

    public static void main(String[] args) {

        if(args.length < 5) {
            System.out.println("args: topology-name file-path spout-parallelism forwarder-parallelism transaction-bolt-parallelism statistics-bolt-parallelism");
            return;
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(Spout, new Spout(args[1]), Integer.parseInt(args[2]));
        builder.setBolt(ForwardBolt, new ForwardBolt(), Integer.parseInt(args[3]))
                .shuffleGrouping(Spout, SELLER_STREAM)
                .shuffleGrouping(Spout, BUYER_STREAM)
                .allGrouping(ControllerBolt, UPSTREAM_COMMAND_STREAM);
        builder.setBolt(ControllerBolt, new ControllerBolt(),1)
                .allGrouping(TransactionBolt, STATE_MIGRATION_STREAM)
                .allGrouping(ForwardBolt, FEEDBACK_STREAM)
                .allGrouping(ForwardBolt, STATISTICS_STREAM)
                .allGrouping(TransactionBolt, STATE_READY_STREAM);

        builder.setBolt(TransactionBolt, new ComputationIntensiveTransactionBolt(), Integer.parseInt(args[4]))
                .directGrouping(ForwardBolt, BUYER_STREAM)
                .directGrouping(ForwardBolt, SELLER_STREAM)
                .directGrouping(ForwardBolt, STATE_MIGRATION_COMMAND_STREAM)
                .directGrouping(ControllerBolt, STATE_UPDATE_STREAM);
        builder.setBolt(StatisticsBolt, new StatisticsBolt(), Integer.parseInt(args[5])).fieldsGrouping(TransactionBolt, TRANSACTION_STREAM, new Fields(SEC_CODE));
        builder.setBolt(LatencyReportBolt, new LatencyReportBolt(), 1).allGrouping(TransactionBolt, PocTopology.LATENCY_REPORT_STREAM);

        Config config = new Config();
        config.setNumWorkers(8);
        try {
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
