package backtype.storm.elasticity;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.elasticity.config.Config;
import backtype.storm.elasticity.metrics.ElasticExecutorMetrics;
import backtype.storm.elasticity.metrics.ExecutionLatencyForRoutes;
import backtype.storm.elasticity.metrics.ThroughputForRoutes;
import backtype.storm.elasticity.routing.BalancedHashRouting;
import backtype.storm.elasticity.routing.PartialHashingRouting;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.scheduler.ElasticScheduler;
import backtype.storm.elasticity.scheduler.model.ExecutorParallelismPredictor;
import backtype.storm.elasticity.scheduler.model.LoadBalancingAwarePredictor;
import backtype.storm.elasticity.scheduler.model.NaivePredictor;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.KeyBucketSampler;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RateTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/4/15.
 */
public class BaseElasticBoltExecutor implements IRichBolt {

//    static final long serialVersionUID = -3216586099702029175L;

    public static Logger LOG = LoggerFactory.getLogger(BaseElasticBoltExecutor.class);

    private BaseElasticBolt _bolt;

    private transient ElasticOutputCollector _outputCollector;

    private transient OutputCollector _originalCollector;

    private transient LinkedBlockingQueue<TupleExecuteResult> _resultQueue;

    private transient Thread _resultHandleThread;

    private transient int _taskId;

    private transient ElasticTasks _elasticTasks;
    private transient ElasticTaskHolder _holder;

    private transient RateTracker _rateTracker;

    private transient ElasticExecutorMetrics metrics;

    public transient KeyBucketSampler _keyBucketSampler;


    public BaseElasticBoltExecutor(BaseElasticBolt bolt) {
        _bolt = bolt;
    }

    public class ResultHandler implements Runnable {

        @Override
        public void run() {
            while(true) {
                try {
                    TupleExecuteResult result = _resultQueue.take();
                    handle(result);
                    LOG.debug("an execution result is emit!");
                }  catch (InterruptedException ee ) {
                    ee.printStackTrace();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void handle(TupleExecuteResult result) {
//            System.out.println("Tuple content: "+result._streamId + " " + result._inputTuple + " "+ result._outputTuple);
            switch (result._commandType) {
                case TupleExecuteResult.Emit:
                    if(result._inputTuple!=null)
                        _originalCollector.emit(result._streamId, result._inputTuple, result._outputTuple);
                    else
                        _originalCollector.emit(result._streamId, result._outputTuple);
                    break;
                case TupleExecuteResult.EmitDirect:
                    if(result._inputTuple!=null)
                        _originalCollector.emitDirect(result._taskId,result._streamId, result._inputTuple, result._outputTuple);
                    else
                        _originalCollector.emitDirect(result._taskId, result._streamId, result._outputTuple);
                    break;
                case TupleExecuteResult.Ack:
                    _originalCollector.ack(result._inputTuple);
                default:
                    assert(false);
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        metrics = new ElasticExecutorMetrics();
        _keyBucketSampler = new KeyBucketSampler(Config.NumberOfShard);
        _resultQueue = new LinkedBlockingQueue<>(Config.ResultQueueCapacity);
        _outputCollector = new ElasticOutputCollector(_resultQueue);
        _bolt.prepare(stormConf, context);
        _originalCollector = collector;
        _resultHandleThread = new Thread(new ResultHandler()) ;
        _resultHandleThread.start();
//        _elasticTasks = new ElasticTasks(_bolt);
//        _elasticTasks.prepare(_outputCollector);
        _taskId = context.getThisTaskId();
        _elasticTasks = ElasticTasks.createHashRouting(1,_bolt,_taskId, _outputCollector);
//        createTest();
//        _elasticTasks = ElasticTasks.createVoidRouting(_bolt, _taskId, _outputCollector);
        _rateTracker = new RateTracker(3000, 5);
        _holder = ElasticTaskHolder.instance();
        if(_holder!=null) {
            _holder.registerElasticBolt(this, _taskId);
        }
        _elasticTasks.get_routingTable().enableRoutingDistributionSampling();
    }

    @Override
    public void execute(Tuple input) {
        try {
        final Object key = _bolt.getKey(input);

        // The following line is comment, as it is used to sample distribution of input streams and the the sampling is only used during the creation of balanced hash routing
        _keyBucketSampler.record(key);

        if(!_elasticTasks.tryHandleTuple(input,key)) {
            System.err.println("elastic task fails to process a tuple!");
            assert(false);
        }
//
//        if(_elasticTasks==null||!_elasticTasks.tryHandleTuple(input,key))
//            _bolt.execute(input, _outputCollector);
        _rateTracker.notify(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {
        _bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public BaseElasticBolt get_bolt() {
        return _bolt;
    }

    public ElasticTasks get_elasticTasks() {
        return _elasticTasks;
    }

    private void createTest() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        System.out.print("Started!");
                        Thread.sleep(10000);
                        LOG.info("Before setting! P="+_elasticTasks.get_routingTable().getNumberOfRoutes());
                        System.out.format("Before setting! P=%d", _elasticTasks.get_routingTable().getNumberOfRoutes());
                        LOG.info("After setting! P="+_elasticTasks.get_routingTable().getNumberOfRoutes());
                        _elasticTasks.setHashRouting(new Random().nextInt(10)+1);
                        _holder.sendMessageToMaster("Task["+_taskId+"] changed is parallelism to "+_elasticTasks.get_routingTable().getNumberOfRoutes());
                        System.out.format("After setting! P=%d", _elasticTasks.get_routingTable().getNumberOfRoutes());
                    }
                } catch (Exception e) {

                }
            }
        }).start();
        System.out.println("testing thread is created!");
    }

    public void insertToResultQueue(TupleExecuteResult result) {
        try {
            _resultQueue.put(result);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public double getInputRate() {
        return _rateTracker.reportRate();
    }

    public ElasticExecutorMetrics getMetrics() {
        RoutingTable routingTable = getCompleteRoutingTable();
        metrics.removeInvalidRoutes(routingTable.getRoutes());
        return metrics;
    }

    public void updateLatencyMetrics(ExecutionLatencyForRoutes latencyForRoutes) {
        metrics.updateLatency(latencyForRoutes);
        RoutingTable routingTable = getCompleteRoutingTable();
        metrics.removeInvalidRoutes(routingTable.getRoutes());
    }

    public void updateThroughputMetrics(ThroughputForRoutes throughputForRoutes) {
        metrics.updateThroughput(throughputForRoutes);
    }


    public int getCurrentParallelism() {
        return getCompleteRoutingTable().getNumberOfRoutes();
    }

    public RoutingTable getCompleteRoutingTable() {
        RoutingTable routingTable = get_elasticTasks().get_routingTable();
        if(routingTable instanceof PartialHashingRouting) {
            routingTable = ((PartialHashingRouting) routingTable).getOriginalRoutingTable();
        }
        return routingTable;
    }

    public int getDesirableParallelism() {

        try {
            final double overProvisioningFactor = 0.5;

            double inputRate = getInputRate();
            Long averageLatency = getMetrics().getRecentAverageLatency(3000);
            if (averageLatency == null) {
                System.out.println("averageLatency is null!");
                return 1;
            }
            double processingRatePerProcessor = 1 / (averageLatency / 1000000000.0);

            RoutingTable routingTable = _elasticTasks.get_routingTable();
            BalancedHashRouting balancedHashRouting = RoutingTableUtils.getBalancecHashRouting(routingTable);
            if (balancedHashRouting == null) {
                return 1;
            }

            long[] routeLoads = ElasticScheduler.getRouteLoads(balancedHashRouting);
            long maxShardLoad = ElasticScheduler.getMaxShardLoad(balancedHashRouting);

//            ExecutorParallelismPredictor predictor = new NaivePredictor();
            ExecutorParallelismPredictor predictor = new LoadBalancingAwarePredictor();


            final int desirableParallelism = predictor.predict(inputRate, balancedHashRouting.getNumberOfRoutes(), processingRatePerProcessor, routeLoads, maxShardLoad);
//            Slave.getInstance().sendMessageToMaster(String.format("Task %d: input rate=%.5f rate per task=%.5f latency: %.5f ms performance factor=%.2f", _taskId, inputRate, processingRatePerProcessor, averageLatency / 1000000.0, ElasticScheduler.getPerformanceFactor(balancedHashRouting)));
            return desirableParallelism;
        } catch (Exception e) {
            e.printStackTrace();
            Slave.getInstance().sendMessageToMaster("Exception happens in getDesirableParallelism()");
            Slave.getInstance().sendMessageToMaster(e.getMessage());
            return 1;
        }
    }

}
