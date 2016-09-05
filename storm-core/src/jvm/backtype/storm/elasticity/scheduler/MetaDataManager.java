package backtype.storm.elasticity.scheduler;

import backtype.storm.elasticity.config.Config;
import backtype.storm.utils.Utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Robert on 9/3/16.
 */
public class MetaDataManager {
    private AtomicLong stateMigrationSize = new AtomicLong(0);

    public MetaDataManager() {
        final int printCyclesInMilliseconds = 1000;
        if(Config.EnableStateMigratedMetrics) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while(true) {
                        Utils.sleep(printCyclesInMilliseconds);
                        System.out.println(String.format("State migrated: %d",stateMigrationSize.get()));
                        stateMigrationSize.set(0);
                    }
                }
            }).start();
        }
    }

    public void reportStateMigration(long size) {
        stateMigrationSize.addAndGet(size);
    }

}
