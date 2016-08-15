package backtype.storm.elasticity.state;

import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Robert on 11/3/15.
 */
public class KeyValueState implements Serializable {

    private Map<Object, Object> state = new ConcurrentHashMap<>();

    public Object getValueByKey(Object key) {
        if (state.containsKey(key))
            return state.get(key);
        else
            return null;
    }

    public synchronized void setValueByKey(Object key, Object value) {
        state.put(key,value);
    }

    public void update(KeyValueState newState) {
        state.putAll(newState.state);
    }

    public void update(Map<Object, Object> newState) {
        state.putAll(newState);
    }

    public Map<Object, Object> getState() {
        return state;
    }

    public synchronized KeyValueState getValidState(StateFilter filter) {
        KeyValueState ret = new KeyValueState();
        for(Object key: state.keySet()) {
            if(filter.isValid(key)) {
                ret.setValueByKey(key, state.get(key));
            }
        }
        return ret;
    }

    public void removeValueByKey(Object key) {
        state.remove(key);
    }

    /**
     * get the state size by serializing the state and measuring the size of bytes after serialization.
     * This is very expensive when the state is large.
     * TODO: using sampling to reduce the overhead of size estimation
     * @return state size
     */
    public long getStateSize() {
        return SerializationUtils.serialize(this).length;
    }
}
