package backtype.storm.elasticity.utils;

import backtype.storm.elasticity.utils.surveillance.ThroughputMonitor;
import backtype.storm.metric.SystemBolt;
import backtype.storm.utils.RateTracker;
import backtype.storm.utils.Utils;
import scala.sys.process.Process;

import java.lang.management.ManagementFactory;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

/**
 * Created by robert on 9/9/16.
 */
public class FrequencyRestrictor {
    int maxFrequencyPerSecond;
    Semaphore semaphore = new Semaphore(0);
    int frequencyPerWindow;
    int millisecondPerWindows;
    int windowsPerSecond;
    public FrequencyRestrictor(int maxFrequencyPerSecond, int windowsPerSecond) {
        this.maxFrequencyPerSecond = maxFrequencyPerSecond;
        this.windowsPerSecond = windowsPerSecond;
        verifyParameters();
        this.frequencyPerWindow = maxFrequencyPerSecond / windowsPerSecond;
        millisecondPerWindows = 1000 / windowsPerSecond;
        new Thread(new Runnable() {
            @Override
            public void run() {
                long lastSleepTime = System.currentTimeMillis();
                long offset = 0;
                long count = 0;
                while(true) {
//                    final long currentTime = System.currentTimeMillis();
//                    Utils.sleep(Math.max(0, millisecondPerWindows - (currentTime - lastSleepTime)));
//                    lastSleepTime = System.currentTimeMillis();
//                    semaphore.release(frequencyPerWindow);
                    Utils.sleep(millisecondPerWindows);
                    final long now = System.currentTimeMillis();
                    semaphore.release((int)((now - lastSleepTime) / (double) millisecondPerWindows * frequencyPerWindow) );
                    lastSleepTime = now;

                }
            }
        }).start();
    }

    public FrequencyRestrictor(int maxFrequencyPerSecond) {
        this(maxFrequencyPerSecond, 50);
    }

    public void getPermission() throws InterruptedException{
        getPermission(1);
    }

    public void getPermission(int numberOfEvents) throws InterruptedException {
        semaphore.acquire(numberOfEvents);
    }

    private void verifyParameters() {
        if(windowsPerSecond > 500 || windowsPerSecond < 1) {
            System.err.println("Wrong number of windowsPerSecond! Default value will be used!");
        }
        windowsPerSecond = Math.max(windowsPerSecond, 1);
        windowsPerSecond = Math.min(windowsPerSecond, 500);
    }

    public static void main(String[] args) {
        System.out.println(ManagementFactory.getRuntimeMXBean().getName());
        final FrequencyRestrictor frequencyRestrictor = new FrequencyRestrictor(1000000,50);
        final RateTracker rateTracker = new RateTracker(5000,10);

//        Timer timer = new Timer("aa",true);
//        timer.scheduleAtFixedRate(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println("Current time: " + System.currentTimeMillis() % 10000);
//            }
//        }, 0, 1000);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        frequencyRestrictor.getPermission(1);
                        rateTracker.notify(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Utils.sleep(1000);
                    System.out.println(rateTracker.reportRate());
                }
            }
        }).start();
    }
}
