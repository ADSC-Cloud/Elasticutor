package backtype.storm.elasticity.scheduler;

import backtype.storm.elasticity.config.Config;
import backtype.storm.utils.Utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Robert on 9/3/16.
 */
public class MetaDataManager {
    private AtomicLong stateMigrationSize = new AtomicLong(0);
    private AtomicLong intraExecutorDataTransferSize = new AtomicLong(0);

    public MetaDataManager() {
        final int printCyclesInMilliseconds = 3000;
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Utils.sleep(printCyclesInMilliseconds);

                    if (Config.EnableStateMigratedMetrics) {
                        System.out.println(String.format("State migrated: %d", stateMigrationSize.getAndSet(0)));
                    }

                    if(Config.EnableIntraExecutorDataTransferMetrics) {
                        System.out.println(String.format("Intra-Executor data transfer: %d", intraExecutorDataTransferSize.getAndSet(0)));
                    }
                }
            }
        }).start();
        System.out.println("State migration display thread is created!");

    }

    public void reportStateMigration(long size) {
        stateMigrationSize.addAndGet(size);
    }

    public void reportIntraExecutorDataTransfer(long size) {
        intraExecutorDataTransferSize.addAndGet(size);
    }

}
