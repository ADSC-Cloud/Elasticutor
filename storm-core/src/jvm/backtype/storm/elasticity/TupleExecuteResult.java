package backtype.storm.elasticity;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Robert on 11/4/15.
 */
public class TupleExecuteResult implements Serializable{


    transient public static final int Emit = 0;
    transient public static final int EmitDirect = 1;
    transient public static final int Ack = 2;

    transient public int _taskId;

    transient public String _streamId;

    transient public Tuple _inputTuple;

    transient public List<Object> _outputTuple;

    transient public int _commandType;


    public TupleExecuteResult(int taskId, String streamId, Tuple inputTuple, List<Object> outputTuple, int command) {
        _taskId = taskId;
        _streamId = streamId;
        _inputTuple = inputTuple;
        _outputTuple = outputTuple;
        _commandType = command;
    }



    public static TupleExecuteResult createEmit(String streamId, Tuple inputTuple, List<Object> outputTuple) {
        return new TupleExecuteResult(0, streamId, inputTuple, outputTuple, Emit);
    }

    public static TupleExecuteResult createEmitDirect(int taskId, String streamId, Tuple inputTuple, List<Object> outputTuple) {
        return new TupleExecuteResult(taskId, streamId, inputTuple, outputTuple, EmitDirect);
    }

    public static TupleExecuteResult createAck(Tuple tuple) {
        return new TupleExecuteResult(-1, null, tuple, null, Ack);
    }

    public String toString() {
        return String.format("TupleExecuteResult: TaskID: %d, StreamID: %s, CommandType: %d, inputTuple: %s, outputTuple: %s", _taskId, _streamId, _commandType, _inputTuple, _outputTuple);
    }

}
