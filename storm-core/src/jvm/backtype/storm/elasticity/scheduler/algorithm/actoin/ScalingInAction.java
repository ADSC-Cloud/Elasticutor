package backtype.storm.elasticity.scheduler.algorithm.actoin;


/**
 * Created by robert on 16-8-15.
 */
public class ScalingInAction extends SchedulingAction {
    public int route;
    public boolean direct;
    public ScalingInAction(int taskid, int route, boolean direct) {
        this.taskID = taskid;
        this.route = route;
        this.direct = direct;
    }
    public ScalingInAction(int taskid, int route) {
        this(taskid, route, true);
    }
    public String toString() {
        return String.format("Scaling in: %d.%d", taskID, route);
    }
}
