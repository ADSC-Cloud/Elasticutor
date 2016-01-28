package backtype.storm.elasticity;

import backtype.storm.elasticity.message.taksmessage.RemoteTupleExecuteResult;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Robert on 11/14/15.
 */
public class RemoteElasticOutputCollector extends ElasticOutputCollector {

    int _originalTaskId;

    public RemoteElasticOutputCollector(LinkedBlockingQueue outputQueue, int originalTaskId) {
        super(outputQueue);
        _originalTaskId = originalTaskId;
    }


    // As the emit is delayed, the destination tasks are unknown.
    @Override
    public List<Integer> emit(String streamId, Tuple inputTuple, List<Object> tuple) {
        try {
            _outputQueue.put(new RemoteTupleExecuteResult(_originalTaskId, 0, streamId, inputTuple, tuple, RemoteTupleExecuteResult.Emit));
        } catch (InterruptedException e) {
            System.out.println("_outputQueue.put() is interrupted!");
        }
        return null;
    }

    @Override
    public List<Integer> emit(Tuple inputTuple, List<Object> tuple) {
        try {
            _outputQueue.put(new RemoteTupleExecuteResult(_originalTaskId, 0,Utils.DEFAULT_STREAM_ID, inputTuple, tuple, RemoteTupleExecuteResult.Emit));
        } catch (InterruptedException e) {
            System.out.println("_outputQueue.put() is interrupted!");
        }
        return null;
    }

    @Override
    public List<Integer> emit(List<Object> tuple) {
        try {
            _outputQueue.put(new RemoteTupleExecuteResult(_originalTaskId, 0,Utils.DEFAULT_STREAM_ID, null, tuple, RemoteTupleExecuteResult.Emit));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple) {
        assert(false);
    }

    @Override
    public void reportError(Throwable error) {

    }

}
