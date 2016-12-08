package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.tuple.Tuple;


/**
 * Created by Robert on 11/15/15.
 */
public class RemoteTuple implements ITaskMessage {

    public int _taskId;
    public transient int _route;
    public Tuple _tuple;

//    public long sid;

    public RemoteTuple(int taskid, int route, Tuple tuple) {
        _taskId = taskid;
        _route = route;
        if(route < 0) throw new RuntimeException("route is " + route);
        _tuple = tuple;
    }

    public String taskIdAndRoutePair() {
        return _taskId+"."+_route;
    }

    public String toString() {
        String ret = "";
        ret += _tuple.toString();
        return ret;
    }

}
