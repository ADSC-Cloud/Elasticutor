package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by acelzj on 03/05/16.
 */
public class ResourceCentricZipfComputationTopology {
    public static String StateMigrationCommandStream = "StateMigrationCommand";
    public static String StateMigrationStream = "StateMigration";
    public static String StateUpdateStream = "StateUpdate";
    public static String StateReadyStream = "SteateReady";
    public static String FeedbackStream = "FeedbackStream";
    public static String RateAndLatencyReportStream = "LatencyAndRateReport";
    public static String SeedUpdateStream = "SeedUpdateStream";
    public static String CountReportSteram = "CountReportStream";
    public static String CountPermissionStream = "CountPermissionStream";

    static String PuncutationEmitStream = "PunctuationEmitStream";
    static String PuncutationFeedbackStreawm = "PunctuationFeedbackStream";


    public static String UpstreamCommand = "UpstreamCommand";

    public static String Spout = "spout";
    public static String GeneratorBolt = "generator";
    public static String ComputationBolt = "computation";
    public static String Controller = "controller";

    public static void main(String[] args) throws Exception {

        if(args.length == 0) {
            System.out.println("args: topology-name number-of-keys skewness sleep-date-in-millis-of-generator-bolt number-of-task-of-generator-bolt sleep-date-in-millisec-of-computation-bolt number-of-task-of-computatoin-bolt");
        }

        TopologyBuilder builder = new TopologyBuilder();

        if(args.length < 3) {
            System.out.println("the number of args should be at least 1");
            return;
        }
        builder.setSpout(Spout, new ZipfSpout(Integer.parseInt(args[1]), Double.parseDouble(args[2])), 1);

        builder.setBolt(GeneratorBolt, new ResourceCentricGeneratorBolt(Integer.parseInt(args[3])),Integer.parseInt(args[4]))
                .allGrouping(Spout)
                .allGrouping(Controller, UpstreamCommand)
                .allGrouping(Controller, SeedUpdateStream)
                .allGrouping(Controller, CountPermissionStream)
                .directGrouping(ComputationBolt, PuncutationFeedbackStreawm);


        builder.setBolt(ComputationBolt, new ResourceCentricComputationBolt(Integer.parseInt(args[5])), Integer.parseInt(args[6]))
                .directGrouping(GeneratorBolt)
                .directGrouping(GeneratorBolt, StateMigrationCommandStream)
                .directGrouping(Controller, StateUpdateStream)
                .directGrouping(GeneratorBolt, PuncutationEmitStream);

        builder.setBolt(Controller, new ResourceCentricControllerBolt(), 1)
                .allGrouping(ComputationBolt, StateMigrationStream)
                .allGrouping(ComputationBolt, StateReadyStream)
                .allGrouping(GeneratorBolt, FeedbackStream)
                .allGrouping(GeneratorBolt, "statics")
                .allGrouping(ComputationBolt, RateAndLatencyReportStream)
                .allGrouping(GeneratorBolt, CountReportSteram);


        Config conf = new Config();
        //   if(args.length>2&&args[2].equals("debug"))
        //       conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
    }
}
