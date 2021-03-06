package storm.starter.elasticity;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by robert on 22/12/16.
 */
public class ElasticityStateCorrectionIncrementalValidationTopologyWithMannualACK {
    public static void main(String[] args) throws Exception {

        if(args.length != 5) {
            System.err.println("topology-name number-of-keys computation-cost-in-nanoseconds number-of-workers " +
                    "acked[1: enable, others: disable]");
            return;
        }

        final String topologyName = args[0];
        final int numberOfKeys = Integer.parseInt(args[1]);
        final int computationCostInNanoSeconds = Integer.parseInt(args[2]);
        final int numberOfWorkers = Integer.parseInt(args[3]);
        final boolean acked = Integer.parseInt(args[4]) == 1;

        if (acked)
            System.out.println("Ack is enabled in the topology!");
        else
            System.out.println("Ack is disabled in the topology!");

        TopologyBuilder builder = new TopologyBuilder();

        builder.setBolt("generator", new IncrementalValidatorBoltWithMannualACK(numberOfKeys, 10000, 100000), 1)
            .directGrouping("bolt", "backpressure");

        builder.setBolt("bolt", new IncrementalStateValidatorElasticBoltWithMannualACK(computationCostInNanoSeconds, acked), 2)
                .fieldsGrouping("generator", new Fields("key"))
                .shuffleGrouping("generator", "backpressure");

        Config conf = new Config();

        conf.setNumWorkers(numberOfWorkers);

        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    }
}
