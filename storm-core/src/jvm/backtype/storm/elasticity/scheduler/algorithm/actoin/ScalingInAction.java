package backtype.storm.elasticity.scheduler.algorithm.actoin;


/**
 * Created by robert on 16-8-15.
 */
public class ScalingInAction implements ScheduingAction {
    public int taskid;
    public ScalingInAction(int taskid) {
        this.taskid = taskid;
    }
}
