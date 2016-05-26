package storm.starter.poc;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.tuple.Values;

/**
 * Created by robert on 25/5/16.
 */
public class Spout extends BaseRichSpout {

    String fileName;
    String outputStreamName;
    transient BufferedReader reader;

    public Spout(String fileName) {
        this.fileName = fileName;
    }

    SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            if(reader == null) {
                reader = new BufferedReader(new FileReader(fileName));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    @Override
    public void nextTuple() {
//        Utils.sleep(1);
        try {
            String line = reader.readLine();
            String[] values = line.split("\\|");

            long orderNo = Long.parseLong(values[0]);
            String acct_id = values[13];
            int secCode = Integer.parseInt(values[11]);
            double price = Double.parseDouble(values[8]);
            int volume = (int)Double.parseDouble(values[10]);
            String time = values[3];

//            for(int i=0;i<values.length;i++) {
//                System.out.println(String.format("%d: %s", i, values[i]));
//            }

            String direction = values[22];
            if(direction.equals("S")) {
                collector.emit(PocTopology.SELLER_STREAM,new Values(orderNo, acct_id, secCode, price, volume, time));
            } else {
                collector.emit(PocTopology.BUYER_STREAM,new Values(orderNo, acct_id, secCode, price, volume, time));
            }



        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(PocTopology.BUYER_STREAM, new Fields(PocTopology.ORDER_NO, PocTopology.ACCT_ID, PocTopology.SEC_CODE, PocTopology.PRICE, PocTopology.VOLUME, PocTopology.TIME));
        declarer.declareStream(PocTopology.SELLER_STREAM, new Fields(PocTopology.ORDER_NO, PocTopology.ACCT_ID, PocTopology.SEC_CODE, PocTopology.PRICE, PocTopology.VOLUME, PocTopology.TIME));
    }
}
