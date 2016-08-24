package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.generated.MasterService;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import storm.starter.surveillance.ThroughputMonitor;

import java.util.Map;
import java.util.Random;


/**
 * Created by acelzj on 03/05/16.
 */
public class ZipfSpout extends BaseRichSpout implements ChangeDistributionService.Iface{
    SpoutOutputCollector _collector;
    int _numberOfElements;
    double _exponent;
    static ZipfSpout _instance;
    Thread _changeDistributionThread;
    long _sleepTimeInMilics;
    int _seed;
    public ZipfSpout(){

    }

    public ZipfSpout(int keys, double skewness) {
        this._numberOfElements = keys;
        this._exponent = skewness;
    }

    public class ChangeDistribution implements Runnable {
        public void run() {
            Random random = new Random(0);
            while (true) {
                Utils.sleep(_sleepTimeInMilics);
                _seed = Math.abs(random.nextInt());
                _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), _seed));
            }
        }
    }

    public void changeNumberOfElements(int numberofElements) throws TException{
        _numberOfElements = numberofElements;
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), (_seed)));
    }

    public void changeExponent(double exponent) throws TException{
        _exponent = exponent;
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), (_seed)));
    }

    public void createThriftServiceThread() {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ChangeDistributionService.Processor processor = new ChangeDistributionService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(9080);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
                    Slave.getInstance().logOnMaster("Hello");
//                    log("Starting the changeDistribution daemon...");
                    server.serve();
                    Slave.getInstance().logOnMaster("ThriftServiceThread.started");
                } catch (TException e) {
                    e.printStackTrace();
                }

            }
        }).start();
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        _collector = collector;
//        _numberOfElements = 1000;
//        _exponent = 1;
        _instance = this;
        _sleepTimeInMilics = 30000;
        _seed = Math.abs(new Random().nextInt());
     //   createThriftServiceThread();
        _collector.emit(new Values(String.valueOf(_numberOfElements), String.valueOf(_exponent), (_seed)));
     //   createThriftServiceThread();
        System.out.println("emited");
        _changeDistributionThread = new Thread(new ChangeDistribution());
        _changeDistributionThread.start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberOfElements", "exponent", "seed"));
    }

    public void nextTuple(){

    }
}
