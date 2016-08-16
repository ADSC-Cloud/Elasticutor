package backtype.storm.elasticity.scheduler.algorithm.actoin;

/**
 * Created by robert on 16-8-15.
 */
public class ScaingOutAction extends SchedulingAction {
    public String targetIP;
    public ScaingOutAction(int taskid, String targetIP) {
        this.taskID = taskid;
        this.targetIP = targetIP;
    }
    public String toString() {
        return String.format("Scaling out: %d -> %s", taskID, targetIP);
    }
}
