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
public class ScalingOutAndInSubtask {
    public static void main(String[] args) {
        if(args.length != 2 && args.length != 3) {
            System.out.println("args: task-id max-parallelism [repeat]");
            return;
        }

        int repeat = 1;
        if(args.length == 3) {
            repeat = Integer.parseInt(args[2]);
        }
        int taskid = Integer.parseInt(args[0]);
        int maxParallelism = Integer.parseInt(args[1]);

        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,9090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            MasterService.Client thriftClient = new MasterService.Client(protocol);

            while(repeat-- > 0) {
                for(int p = 1; p < maxParallelism; p++) {
                    thriftClient.scalingOutSubtask(taskid);
                    System.out.println(String.format("Scaled out, parallelism = %d", p + 1));
                }
                for(int p = maxParallelism; p > 1; p --) {
                    thriftClient.scalingInSubtask(taskid);
                    System.out.println(String.format("Scaled in, parallelism = %d", p - 1));
                }
                System.out.println("=====================");
            }


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
