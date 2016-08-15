package backtype.storm.elasticity.scheduler.algorithm.actoin;

/**
 * Created by robert on 16-8-15.
 */
public class ScaingOutAction implements ScheduingAction {
    public int taskid;
    public String targetIP;
    public ScaingOutAction(int taskid, String targetIP) {
        this.taskid = taskid;
        this.targetIP = targetIP;
    }
}
