package backtype.storm.elasticity.utils;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.utils.Utils;
import org.eclipse.jetty.util.BlockingArrayQueue;

import java.util.*;

/**
 * Created by robert on 8/12/16.
 */
public class MonitorUtils {
    private Map<Long, ThreadCpuUtilization.Context> thread2Contex = new HashMap<>();
    private Map<Long, Timer> thread2Timer = new HashMap<>();
    private Map<Queue, Timer> queue2Timer = new HashMap<>();
    private static MonitorUtils instance;
    static public MonitorUtils instance() {
        if(instance == null)
            instance = new MonitorUtils();
        return instance;
    }

    public void registerQueueMonitor(final Queue queue, final String name, final int capacity, final Double thresholdLow,
                                     final Double thresholdHigh, final int cyclesInSeconds) {
        if(queue2Timer.containsKey(queue))
            return;
        final Timer timer = new Timer();
        queue2Timer.put(queue, timer);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                double utilization = (double) queue.size() / capacity;
                if (thresholdLow != null && utilization < thresholdLow)
                    Slave.getInstance().logOnMaster(String.format("Utilization of queue [%s] reaches %f", name,
                            utilization));
                if (thresholdHigh != null && utilization > thresholdHigh)
                    Slave.getInstance().logOnMaster(String.format("Utilization of queue [%s] reaches %f", name,
                            utilization));
            }
        }, 0, cyclesInSeconds * 1000);

    }

    private void registerQueueMonitorTest(final Queue queue, final String name, final int capacity, final Double thresholdLow,
                                     final Double thresholdHigh, final int cyclesInSeconds) {
        if(queue2Timer.containsKey(queue))
            return;
        final Timer timer = new Timer();
        queue2Timer.put(queue, timer);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                double utilization = (double) queue.size() / capacity;
                if (thresholdLow != null && utilization < thresholdLow)
                    System.out.println(String.format("Utilization of queue [%s] reaches %f", name,
                            utilization));
                if (thresholdHigh != null && utilization > thresholdHigh)
                    System.out.println(String.format("Utilization of queue [%s] reaches %f", name,
                            utilization));
            }
        }, 0, cyclesInSeconds * 1000);

    }

    public void registerThreadMonitor(final long threadId, final String threadName, final double reportThreshold,
                                      final int delayInSeconds) {
        if(thread2Contex.containsKey(threadId))
            return;
        thread2Contex.put(threadId, new ThreadCpuUtilization.Context(threadId));
        final Timer timer = new Timer();
        thread2Timer.put(threadId, timer);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                ThreadCpuUtilization.Context context = thread2Contex.get(threadId);
                if(context != null) {
                    double utilization = ThreadCpuUtilization.getCpuUtilizationRate(context);
                    if (utilization > reportThreshold) {
                        Slave.getInstance().logOnMaster(String.format("Utilization of thread [%s] reaches %f!", threadName, utilization));
                    }
                }
            }
        }, 0, delayInSeconds * 1000);

    }

    private void registerThreadMonitorTest(final long threadId, final String threadName, final double reportThreshold,
                                          final int delayInSeconds) {
        if(thread2Contex.containsKey(threadId))
            return;
        thread2Contex.put(threadId, new ThreadCpuUtilization.Context(threadId));
        final Timer timer = new Timer();
        thread2Timer.put(threadId, timer);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Executed!");
                ThreadCpuUtilization.Context context = thread2Contex.get(threadId);
                if(context != null) {
                    double utilization = ThreadCpuUtilization.getCpuUtilizationRate(context);
                    if (utilization > reportThreshold) {
                        System.out.println(String.format("Utilization of thread [%s] reaches %f!", threadName, utilization));
                    }
                }
            }
        }, 0, delayInSeconds * 1000);

    }

    public void unregister(final long threadId) {
        thread2Contex.remove(threadId);
        if(thread2Timer.containsKey(threadId)) {
            thread2Timer.get(threadId).cancel();
            thread2Timer.remove(threadId);
        }
    }
    public static long compute(long timeInNanosecond) {
        final long start = System.nanoTime();
        long seed = start;
        while(System.nanoTime() - start < timeInNanosecond) {
            seed = (long) Math.sqrt(3.15);
        }
        return seed;
    }

    public static void main(String[] args) {
//        MonitorUtils.instance().registerThreadMonitorTest(Thread.currentThread().getId(), "current: ", -1.0, 1);
//        compute(3000000000L);
//        MonitorUtils.instance().unregister(Thread.currentThread().getId());
//        compute(3000000000L);
//        MonitorUtils.instance().registerThreadMonitorTest(Thread.currentThread().getId(), "current: ", -1.0, 1);

        BlockingArrayQueue<Integer> queue = new BlockingArrayQueue<>(10);
        MonitorUtils.instance().registerQueueMonitorTest(queue, "Test Queue", 10, 0.4, 0.6, 1);

        Utils.sleep(2000);
        queue.add(1);
        queue.add(2);
        queue.add(3);

        Utils.sleep(2000);
        queue.add(4);
        queue.add(5);
        queue.add(6);
        Utils.sleep(3000);
        queue.add(7);
        queue.add(8);
        queue.add(9);
    }

}
