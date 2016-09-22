package backtype.storm.elasticity.utils.serialize;

import backtype.storm.elasticity.message.taksmessage.RemoteTupleExecuteResult;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by robert on 16-7-29.
 */
public class RemoteTupleExecuteResultSerializer {

    KryoValuesSerializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Output _kryoOut;
    KryoTupleSerializer tupleSerializer;

    public RemoteTupleExecuteResultSerializer(final Map conf, final GeneralTopologyContext context, KryoTupleSerializer tupleSerializer) {
        _kryo = new KryoValuesSerializer(conf);
        _kryoOut = new Output(2000, 2000000000);
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        this.tupleSerializer = tupleSerializer;
    }

    public byte[] serialize(RemoteTupleExecuteResult tuple) {
        try {

            _kryoOut.clear();
            _kryoOut.writeInt(tuple._taskId);
            _kryoOut.writeString(tuple._streamId);
            byte[] bytes = null;
            int byteLength = 0;
            if(tuple._inputTuple!=null) {
                bytes = tupleSerializer.serialize(tuple._inputTuple);
                byteLength = bytes.length;
            }
            _kryoOut.writeInt(byteLength);
            if(byteLength != 0)
                _kryoOut.writeBytes(bytes);
            if(tuple._outputTuple == null) {
                _kryo.serializeInto(new ArrayList<Object>(), _kryoOut);
            } else {
                _kryo.serializeInto(tuple._outputTuple, _kryoOut);
            }
            _kryoOut.writeInt(tuple._commandType);
            _kryoOut.writeInt(tuple._originalTaskID);
            return _kryoOut.toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
