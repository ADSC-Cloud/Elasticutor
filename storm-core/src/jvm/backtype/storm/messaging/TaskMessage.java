/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.messaging;

import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class TaskMessage implements Serializable {
    private int _task;
    public short remoteTuple = -1;
    private byte[] _message;

    public TaskMessage(int task, byte[] message) {
        _task = task;
        _message = message;
    }

    public TaskMessage(int task, short remoteTuple, byte[] message) {
        this(task, message);
        this.remoteTuple = remoteTuple;
    }

    public int task() {
        return _task;
    }

    public byte[] message() {
        return _message;
    }
    
    public ByteBuffer serialize() {
        ByteBuffer bb = ByteBuffer.allocate(_message.length+4);
        bb.putShort((short)_task);
        bb.putShort(remoteTuple);
        bb.put(_message);
        return bb;
    }
    
    public void deserialize(ByteBuffer packet) {
        if (packet==null) return;
        _task = packet.getShort();
        remoteTuple = packet.getShort();
        _message = new byte[packet.limit()-4];
        packet.get(_message);
    }

    public void setRemoteTuple() {
        remoteTuple = 1;
    }

    public String toString() {
        return String.format("task = %d, remoteTuple = %d, message size = %d", _task, remoteTuple, _message.length);
    }

    public static void main(String[] args) {
        String content = "Content";
        TaskMessage taskMessage = new TaskMessage(100, SerializationUtils.serialize(content));
        taskMessage.setRemoteTuple();
        ByteBuffer bytes = taskMessage.serialize();



//        System.out.println("first: " + bytes.getShort(0));
//        System.out.println("second: " + bytes.getShort(2));
        TaskMessage taskMessage1 = new TaskMessage(1,null);
        bytes.position(0);
        taskMessage1.deserialize(bytes);

        System.out.println(taskMessage1);
    }

}
