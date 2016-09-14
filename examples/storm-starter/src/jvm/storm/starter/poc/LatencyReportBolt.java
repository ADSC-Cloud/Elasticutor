package storm.starter.poc;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by robert on 2/9/16.
 */
public class LatencyReportBolt extends BaseRichBolt {

    List<Long> latencyHistory;



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        latencyHistory = new ArrayList<>();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    Utils.sleep(10000);
                    synchronized (latencyHistory) {

                        if(latencyHistory.size() == 0) {
                            Slave.getInstance().sendMessageToMaster("Processing Latency: " + 0);
                        } else {
                            long sum = 0;
                            for(long l: latencyHistory) {
                                sum += l;
                            }

                            double avg = sum / latencyHistory.size();

                            double dev = 0;
                            for(long l: latencyHistory) {
                                dev += Math.pow((l - avg), 2);
                            }

                            double standardDev = Math.sqrt(dev);


                            Slave.getInstance().sendMessageToMaster(String.format("Processing Latency: %.2f, standard dev: %.2f", avg, standardDev));
                            latencyHistory.clear();
                        }
                    }
                }
            }
        }).start();
    }

    @Override
    public void execute(Tuple input) {
        synchronized (latencyHistory) {
            latencyHistory.add(input.getLong(0));
        }
    }
}
