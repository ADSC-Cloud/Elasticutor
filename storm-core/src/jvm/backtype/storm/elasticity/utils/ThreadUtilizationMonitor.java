package backtype.storm.elasticity.utils;

import backtype.storm.elasticity.actors.Slave;
import backtype.storm.utils.Utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Created by robert on 8/12/16.
 */
public class ThreadUtilizationMonitor {
    Map<Long, ThreadCpuUtilization.Context> thread2Contex = new HashMap<>();
    Map<Long, Timer> thread2Timer = new HashMap<>();
    private static ThreadUtilizationMonitor instance;
    static public ThreadUtilizationMonitor instance() {
        if(instance == null)
            instance = new ThreadUtilizationMonitor();
        return instance;
    }
    public void registerMonitor(final long threadId, final String threadName, final double reportThreshold,
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
//                        Slave.getInstance().logOnMaster(String.format("Utilization of thread [%s] reaches %f!", threadName, utilization));
                    }
                }
            }
        }, 0, delayInSeconds * 1000);

    }

    public void registerMonitorTest(final long threadId, final String threadName, final double reportThreshold,
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

//            try {
//                TimeUnit.NANOSECONDS.sleep(timeInNanosecond);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//                return 0;
    }

    public static void main(String[] args) {
        ThreadUtilizationMonitor.instance().registerMonitorTest(Thread.currentThread().getId(), "current: ", -1.0, 1);
        compute(3000000000L);
        ThreadUtilizationMonitor.instance().unregister(Thread.currentThread().getId());
        compute(3000000000L);
        ThreadUtilizationMonitor.instance().registerMonitorTest(Thread.currentThread().getId(), "current: ", -1.0, 1);
    }

}
