package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by acelzj on 03/05/16.
 */
public class ResourceCentricZipfComputationTopology {
    static String StateMigrationCommandStream = "StateMigrationCommand";
    static String StateMigrationStream = "StateMigration";
    static String StateUpdateStream = "StateUpdate";
    static String StateReadyStream = "SteateReady";
    static String FeedbackStream = "FeedbackStream";
    static String RateAndLatencyReportStream = "LatencyAndRateReport";
    static String SeedUpdateStream = "SeedUpdateStream";
    static String CountReportSteram = "CountReportStream";
    static String CountPermissionStream = "CountPermissionStream";

    static String PuncutationEmitStream = "PunctuationEmitStream";
    static String PuncutationFeedbackStreawm = "PunctuationFeedbackStream";


    static String UpstreamCommand = "UpstreamCommand";
    static String Spout = "spout";
    static String GeneratorBolt = "generator";
    static String ComputationBolt = "computation";
    static String Controller = "controller";

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

        builder.setBolt(GeneratorBolt, new ResourceCentricGeneratorBolt(Integer.parseInt(args[3]), Integer.parseInt(args[1]), Double.parseDouble(args[2])),Integer.parseInt(args[4]))
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
