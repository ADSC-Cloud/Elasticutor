package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by acelzj on 03/05/16.
 */
public class ZipfComputationTopology {
    public static void main(String[] args) throws Exception {

        if(args.length == 0) {
            System.out.println("args: topology-name sleep-date-in-millis [debug|any other]");
        }

        TopologyBuilder builder = new TopologyBuilder();

        if(args.length < 4) {
            System.out.println("the number of args should be at least 4");
        }
        builder.setSpout("spout", new ZipfSpout(), 1);

        builder.setBolt("controller", new ZipControlerBolt(), 1).allGrouping("generator", "CountReportStream");

        builder.setBolt("generator", new GeneratorBolt(Integer.parseInt(args[1])),Integer.parseInt(args[2]))
                .allGrouping("spout")
                .allGrouping("controller","CountPermissionStream");

        builder.setBolt("computator", new ComputationBolt(Integer.parseInt(args[3])), Integer.parseInt(args[4])).fieldsGrouping("generator", new Fields("numberOfTask"));

        Config conf = new Config();
        //   if(args.length>2&&args[2].equals("debug"))
        //       conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(8);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
    }
}
