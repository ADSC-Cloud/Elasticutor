package storm.starter.elasticity;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.elasticity.util.StateConsistencyValidator;

/**
 * Created by robert on 22/12/16.
 */
public class ElasticityCorrectionValidationTopology {
    public static void main(String[] args) throws Exception {

        if(args.length != 4) {
            System.err.println("topology-name number-of-keys check-frequency computation-cost-in-nanoseconds");
            return;
        }

        final String topologyName = args[0];
        final int numberOfKeys = Integer.parseInt(args[1]);
        final int checkFrequency = Integer.parseInt(args[2]);
        final int computationCostInNanoSeconds = Integer.parseInt(args[3]);

        TopologyBuilder builder = new TopologyBuilder();

        StateConsistencyValidator stateConsistencyValidator = new StateConsistencyValidator(numberOfKeys, checkFrequency);

        builder.setSpout("spout", new XORValidatorSpout(stateConsistencyValidator));

        builder.setBolt("bolt", new XORValidatorElasticBolt(stateConsistencyValidator, computationCostInNanoSeconds))
                .fieldsGrouping("spout", new Fields("key"));

        Config conf = new Config();

        conf.setNumWorkers(2);

        StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
    }
}
