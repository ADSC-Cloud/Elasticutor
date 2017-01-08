package backtype.storm.elasticity.actors.utils;

import backtype.storm.generated.MasterService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by robert on 1/27/16.
 */
public class ScalingOutSubtask {
    public static void main(String[] args) {
        if(args.length!=1 && args.length!=2) {
            System.out.println("args: task-id [repeated]");
            return;
        }

        int repeat = 1;
        if(args.length == 2) {
            repeat = Integer.parseInt(args[1]);
        }
        backtype.storm.elasticity.config.Config.overrideFromStormConfigFile();
        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,9090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            MasterService.Client thriftClient = new MasterService.Client(protocol);
            while(repeat-- > 0) {
                thriftClient.scalingOutSubtask(Integer.parseInt(args[0]));
            }
            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("finished!");
    }
}
