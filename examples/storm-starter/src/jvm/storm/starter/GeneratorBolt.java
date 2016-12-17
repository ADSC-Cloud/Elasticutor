package storm.starter;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.topology.IRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

import backtype.storm.elasticity.utils.surveillance.ThroughputMonitor;

import backtype.storm.tuple.Values;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.math3.distribution.ZipfDistribution;
/**
 * Created by acelzj on 03/05/16.
 */
public class GeneratorBolt implements IRichBolt{

    ZipfDistribution _distribution;
    transient int count;
    OutputCollector _collector;
    int _numberOfElements;
    double _exponent;
    Thread _emitThread;
    transient ThroughputMonitor monitor;
    int _sleepTimeInMilics;
    int _prime;
    transient long start;
    transient long end;
    int _seed;
    int taskIndex;
    int taskId;
    Random rand;
    final int[] primes = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271};
   /* public class ChangeDistribution implements Runnable (Tuple tuple){

        @Override
        public void run() {
            while (true) {
                setNumberOfElements(tuple);
                Random rand = new Random(seed);
                System.out.println("distribution has been changed");
                _prime = primes[rand.nextInt(primes.length)];
                Slave.getInstance().logOnMaster("distribution has been changed");
            }
        }
    }*/

    long progress = 200;
    public GeneratorBolt(int sleepTimeInMilics) { _sleepTimeInMilics = sleepTimeInMilics; }
    public class emitKey implements Runnable {
        public void run() {
            long count = 0;
            while (true) {
                try {
                //    Slave.getInstance().logOnMaster("Time:"+String.valueOf(_sleepTimeInMilics));
                 //   long BeforeSleep = System.currentTimeMillis();
                    Utils.sleep(_sleepTimeInMilics);
                //    long AfterSleep = System.currentTimeMillis();
                //    Slave.getInstance().logOnMaster("Sleep_Time:"+String.valueOf(AfterSleep-BeforeSleep));
                  //  Thread.sleep(_sleepTimeInMilics);
                    int key = _distribution.sample();
                //    System.out.println("key");
                //    System.out.println(key);
//                    _prime = primes[rand.nextInt(primes.length)];
                    key = ((key + _prime) * 101) % 1113;
                 /*   if(count == 0){
                        start = System.currentTimeMillis();
                    }
                    ++count;
                    if(count == 1000){
                        end = System.currentTimeMillis();
                        Slave.getInstance().logOnMaster("1000:"+String.valueOf(end-start));
                        count %= 1000;
                    //    start = System.currentTimeMillis();
                    }*/

                    while(count >= progress) {
                        Utils.sleep(1);
                    }

                    if(count % 10 ==0) {
                        _collector.emit("CountReportStream", new Values(taskIndex, count));
                    }
                    count++;
                    _collector.emit(new Values(String.valueOf(key)));
                    monitor.rateTracker.notify(1);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("numberOfTask"));
        declarer.declareStream("CountReportStream", new Fields("taskid", "count"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _numberOfElements = 1000;
        _exponent = 1.5;
        _prime = 41;
        count = 0;
        start = 0;
        end = 0;
        monitor = new ThroughputMonitor(""+context.getThisTaskId());
        _distribution = new ZipfDistribution(_numberOfElements, _exponent);
        _seed = 0;
        _emitThread = new Thread(new emitKey());
        _emitThread.start();


        taskId = context.getThisTaskId();
        taskIndex = -1;

        for(int i = 0; i < context.getComponentTasks("generator").size(); i++) {
            if(taskId == context.getComponentTasks("generator").get(i)) {
                taskIndex = i;
            }
        }

     /*   new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(1000);
                        Slave.getInstance().logOnMaster("My throughput:" + monitor.rateTracker.reportRate());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();*/

    }

    public Map getComponentConfiguration(){ return new HashedMap();}

    public void setNumberOfElements(Tuple tuple) {
//        System.out.println(tuple.getString(0));
        _numberOfElements = Integer.parseInt(tuple.getString(0));
    }

    public void setExponent(Tuple tuple) {
//        System.out.println(tuple.getString(1));
        _exponent = Double.parseDouble(tuple.getString(1));
    }

    public void cleanup() { }

    public void execute(Tuple tuple){
      //  setNumberOfElements(tuple);
      //  setExponent(tuple);
        if(tuple.getSourceStreamId().equals(Utils.DEFAULT_STREAM_ID)) {
            _numberOfElements = Integer.parseInt(tuple.getString(0));
            _exponent = Double.parseDouble(tuple.getString(1));
            _seed = tuple.getInteger(2);
            _distribution = new ZipfDistribution(_numberOfElements, _exponent);
    //        rand = new Random(_seed);
            _prime = primes[_seed % primes.length];

            Slave.getInstance().logOnMaster(String.format("Prime changed to %d!", _prime));
        } else if (tuple.getSourceStreamId().equals("CountPermissionStream")) {
            progress = Math.max(progress, tuple.getLong(0));
//            Slave.getInstance().logOnMaster(String.format("Progress on task %d is updated to %d", executorId, progress));
        }
    }

}
