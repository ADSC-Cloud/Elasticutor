package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.message.actormessage.IMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robert on 12/16/15.
 */
public class ShardToRouteReassignment implements IMessage{

    public int taskid;
    public Map<Integer, Integer> reassignment = new HashMap<>();

    public ShardToRouteReassignment(int taskId, int bucketid, int route) {
        this.taskid = taskId;
        reassignment.put(bucketid, route);
    }

    public ShardToRouteReassignment(int taskId, Map<Integer, Integer> reassignment) {
        this.reassignment.putAll(reassignment);
    }
}
