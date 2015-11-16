package backtype.storm.elasticity.ActorFramework.Message;

import backtype.storm.elasticity.ElasticTasks;
import backtype.storm.elasticity.state.*;

/**
 * Created by Robert on 11/12/15.
 */
public class ElasticTaskMigrationMessage implements IMessage {

    public ElasticTasks _elasticTask;

    public int _port;

    public String _ip;

    public KeyValueState state;

    public ElasticTaskMigrationMessage(ElasticTasks task, int port, KeyValueState s) {
        _elasticTask = task;
//        _ip = ip;
        _port = port;
        state = s;
    }

    public String getString() {
        return "source: "+_ip+":"+_port+" task id: "+_elasticTask.get_taskID();
    }
}
