package backtype.storm.elasticity.message.taksmessage;


/**
 * Created by robert on 26/4/16.
 */
public class CleanPendingTupleToken implements ITaskMessage {
    public int executorId;
    public int routeId;
    public CleanPendingTupleToken(int taskid, int routeid) {
        executorId = taskid;
        routeId = routeid;
    }
}
