package backtype.storm.elasticity.actors.utils;

import backtype.storm.generated.MasterService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;

//import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.File;
/**
 * Created by Robert on 11/20/15.
 */
public class QueryThroughput {

    public static void main(String[] args) throws IOException{

        if(args.length!=2) {
            System.out.println("args: taskid, cycle-in-secs");
            return;
        }
     //   File file = new File("/home/acelzj/storm/storm-dist/binary/target/apache-storm-0.11.0-SNAPSHOT/throughput/ThroughtWihtoutLoadBalancingAndAutoScaling.txt");
     //   if (!file.exists()) {
     //       file.createNewFile();
      //  }
      //  FileOutputStream fop = new FileOutputStream(file);
        int taskid = Integer.parseInt(args[0]);
        backtype.storm.elasticity.config.Config.overrideFromStormConfigFile();
        TTransport transport = new TSocket(backtype.storm.elasticity.config.Config.masterIp,9090);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            MasterService.Client thriftClient = new MasterService.Client(protocol);

            int i = 0;
            while(true) {
                Thread.sleep(1000*Integer.parseInt(args[1]));
                try{

                double throughput = thriftClient.reportTaskThroughput(taskid);
           //     String content = "Task "+ taskid + ": "+throughput;
            //        String newline = System.getProperty("line.separator");
            //    byte[] contentInBytes = content.getBytes();
           //     byte[] nextlineInBytes = newline.getBytes();
           //     fop.write(contentInBytes);
           //     fop.write(nextlineInBytes);
                System.out.println("Task "+ taskid + ": "+throughput);

                    if (i%5==0) {
                        System.out.println(thriftClient.queryDistribution(taskid));
                       // fw.write(thriftClient.queryDistribution(taskid));
                       // fw.write(nextlineInBytes);
                    }

                } catch (TException e ) {
                    e.printStackTrace();
                }
                i++;
            }

//            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        fop.flush();
 //       fop.close();
        System.out.println("finished!");
    }
}
