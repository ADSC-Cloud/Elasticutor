package backtype.storm.elasticity.actors.utils;

import backtype.storm.generated.MasterService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by robert on 12/16/15.
 */
public class QueryExecutorStatus {
    public static void main(String[] args) {

        if(args.length!=1) {
            System.out.println("args: taskid");
            return;
        }

        int taskid = Integer.parseInt(args[0]);
        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,9090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            MasterService.Client thriftClient = new MasterService.Client(protocol);
            String result = thriftClient.getExecutorStatus(taskid);
            System.out.println(result);

            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("finished!");
    }
}
