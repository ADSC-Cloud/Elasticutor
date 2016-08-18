package backtype.storm.elasticity.actors.utils;

import backtype.storm.generated.MasterService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by Robert on 11/13/15.
 */
public class MigrateTasks {

    public static void main(String[] args) {
        if(args.length < 3) {
            System.out.println("args: orignal-hostname, target-hostname, task-id, route, repeat (default = 1) ");
            return;
        }

        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,9090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            MasterService.Client thriftClient = new MasterService.Client(protocol);

            int repeat = 1;
            if(args.length >=5)
                repeat = Integer.parseInt(args[4]);
            for(int i = 0; i < repeat; i++) {
                if(i % 2 ==0) {
                    System.out.print(String.format("From %s to %s.   ", args[0], args[1]));
                    thriftClient.migrateTasks(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
                    System.out.println(".......................... Done");
                }
                else {
                    System.out.print(String.format("From %s to %s.   ", args[1], args[0]));
                    thriftClient.migrateTasks(args[0], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
                    System.out.println(".......................... Done");
                }
            }

            transport.close();
        } catch (TException e) {
            e.printStackTrace();
        }
        System.out.println("finished!");
    }
}
