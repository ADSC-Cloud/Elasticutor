package storm.starter.poc;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by robert on 25/5/16.
 */
public class PocTopology {
    public static String SEC_CODE = "sec_code";
    public static String ACCT_ID = "acct_id";
    public static String ORDER_NO = "order_no";
    public static String PRICE = "price";
    public static String VOLUME = "volume";
    public static String DATE = "date";
    public static String TIME = "time";
    public static String MILLISECOND = "millisecond";
    public static String EMIT_TIME_STAMP = "emit_time_stamp";


    public static String BUYER_STREAM = "buyer_stream";
    public static String SELLER_STREAM = "seller_stream";
    public static String TRANSACTION_STREAM = "transaction_stream";
    public static String LATENCY_REPORT_STREAM = "latency_report_stream";


    public static String Spout = "spout";
    public static String TransactionBolt = "TransactionBolt";
    public static String StatisticsBolt = "StatisticsBolt";
    public static String LatencyReportBolt = "LatencyReportBolt";
    public static String ForwardBolt = "ForwardBolt";

    public static void main(String[] args) {

        if(args.length < 4) {
            System.out.println("args: topology-name file-path spout-parallelism transaction-bolt-parallelism statistics-bolt-parallelism");
            return;
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(Spout, new Spout(args[1]), Integer.parseInt(args[2]));
        builder.setBolt(TransactionBolt, new ComputationIntensiveTransactionBolt(), Integer.parseInt(args[3])).fieldsGrouping(Spout, BUYER_STREAM, new Fields(SEC_CODE)).fieldsGrouping(Spout, SELLER_STREAM, new Fields(SEC_CODE));
        builder.setBolt(StatisticsBolt, new StatisticsBolt(), Integer.parseInt(args[4])).fieldsGrouping(TransactionBolt, TRANSACTION_STREAM, new Fields(SEC_CODE));
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
