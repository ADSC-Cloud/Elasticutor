package backtype.storm.elasticity.message.taksmessage;

import backtype.storm.elasticity.TupleExecuteResult;
import backtype.storm.elasticity.utils.serialize.RemoteTupleExecuteResultDeserializer;
import backtype.storm.elasticity.utils.serialize.RemoteTupleExecuteResultSerializer;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.Tuple;

import java.util.List;

/**
 * Created by Robert on 11/14/15.
 */
public class RemoteTupleExecuteResult extends TupleExecuteResult implements ITaskMessage {

    transient public int _originalTaskID;

    transient public boolean serialized;

//    private byte[] inputTupleBytes;
//    private byte[] outputTupleBytes;

    public byte[] bytes;

    public RemoteTupleExecuteResult(int orignalTaskID, int taskId, String streamId, Tuple inputTuple, List<Object> outputTuple, int command) {
        super(taskId,streamId,inputTuple,outputTuple,command);
        _originalTaskID = orignalTaskID;
    }

//    public void serialize(KryoTupleSerializer serializer) {
//        inputTupleBytes = serializer.serialize(_inputTuple);
//        _inputTuple = null;
//
//        outputTupleBytes = serializer.serialize(_outputTuple);
//        _outputTuple = null;
//
//        serialized = true;
//    }
//
//    public void deserialized(KryoTupleDeserializer deserializer) {
//        _inputTuple = deserializer.deserialize(inputTupleBytes);
//        inputTupleBytes = null;
//
//        _outputTuple = deserializer.deserializeObjects(outputTupleBytes);
//        outputTupleBytes = null;
//        serialized = false;
//    }

    public void spaceEfficientSerialize(RemoteTupleExecuteResultSerializer serializer) {
        bytes = serializer.serialize(this);
    }

    public void spaceEfficientDeserialize(RemoteTupleExecuteResultDeserializer deserializer) {
        deserializer.deserialize(this);
    }


    public String toString() {
        String ret = "RemoteTupleExecuteResult:";
        ret += String.format("_originalTaskId = %s\n", _originalTaskID);
        ret += super.toString();

        return ret;
    }


}
