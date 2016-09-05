package backtype.storm.elasticity.metrics;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by robert on 5/9/16.
 */
public class WorkerMetrics {

    public static WorkerMetrics instance;

    static public WorkerMetrics getInstance() {
        if(instance == null) {
            instance = new WorkerMetrics();
        }
        return instance;
    }

    public List<Long> stateMigrationSizeHistory = new ArrayList<Long>();
    public List<Long> intraExecutorDataTransferSize = new ArrayList<Long>();

    synchronized public void recordStateMigration(long size) {
        stateMigrationSizeHistory.add(size);
    }

    synchronized public void recordRemoteTaskTupleOrExecutionResultTransfer(long size) {
        intraExecutorDataTransferSize.add(size);
    }

    synchronized public long getStateMigrationSizeAndReset() {
        long sum = 0;
        for(long s: stateMigrationSizeHistory) {
            sum += s;
        }
        stateMigrationSizeHistory.clear();
        return sum;
    }

    synchronized public long getDataTransferSizeAndReset() {
        long sum = 0;
        for(long s: intraExecutorDataTransferSize) {
            sum += s;
        }
        intraExecutorDataTransferSize.clear();
        return sum;
    }

}
