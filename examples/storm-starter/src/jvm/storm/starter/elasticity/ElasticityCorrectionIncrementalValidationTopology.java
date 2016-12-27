package storm.starter.elasticity;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.elasticity.util.StateConsistencyValidator;

/**
 * Created by robert on 22/12/16.
 */
public class ElasticityCorrectionIncrementalValidationTopology {
    public static void main(String[] args) throws Exception {

        if(args.length != 3) {
            System.err.println("topology-name number-of-keys computation-cost-in-nanoseconds");
            return;
        }

        final String topologyName = args[0];
        final int numberOfKeys = Integer.parseInt(args[1]);
        final int computationCostInNanoSeconds = Integer.parseInt(args[2]);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new IncrementalValidatorSpout(numberOfKeys));

        builder.setBolt("bolt", new IncrementalValidatorElasticBolt(computationCostInNanoSeconds))
                .fieldsGrouping("spout", new Fields("key"));

        Config conf = new Config();

        conf.setNumWorkers(1);

        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    }
}
