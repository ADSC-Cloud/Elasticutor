package backtype.storm.elasticity.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.message.actormessage.*;
import backtype.storm.elasticity.resource.ResourceManager;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.scheduler.ElasticScheduler;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.generated.HostNotExistException;
import backtype.storm.generated.MasterService;
import backtype.storm.generated.MigrationException;
import backtype.storm.generated.TaskNotExistException;
import backtype.storm.utils.Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Robert on 11/11/15.
 */
public class Master extends UntypedActor implements MasterService.Iface {

    Cluster cluster = Cluster.get(getContext().system());

//    private Map<String, ActorRef> _nameToActors = new HashMap<>();

    private Map<String, ActorPath> _nameToPath = new ConcurrentHashMap<>();

    private Map<Integer, String> _taskidToActorName = new HashMap<>();

    public Map<Integer, String> _elasticTaskIdToWorker = new HashMap<>();

    public Map<String, String> _taskidRouteToWorker = new HashMap<>();

    private Map<String, String> _hostNameToWorkerLogicalName = new HashMap<>();

//    IConnection _loggingInput;

    static Master _instance;

    public static Master getInstance() {
        return _instance;
    }

    public Master() {
        _instance = this;
        createThriftServiceThread();
    }

//    public void deployLoggingServer() {
//        _loggingInput = new MyContext().bind("logging", backtype.storm.elasticity.config.Config.LoggingServerPort);
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while(true) {
//                    Iterator<TaskMessage> iterator = _loggingInput.recv(0, 0);
//                    while(iterator.hasNext()) {
//                        TaskMessage message = iterator.next();
//
//                    }
//                }
//            }
//        })
//    }

    public void createThriftServiceThread() {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    MasterService.Processor processor = new MasterService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(9090);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

                    log("Starting the monitoring daemon...");
                    server.serve();
                } catch (TException e) {
                    e.printStackTrace();
                }

            }
        }).start();
    }

    private String getWorkerLogicalName(String hostName) {
        if(_hostNameToWorkerLogicalName.containsKey(hostName)) {
            return _hostNameToWorkerLogicalName.get(hostName);
        }
        else {
            return "Unknown worker name";
        }
    }

    private String getHostByWorkerLogicalName(String worker) {
        for(String key: _hostNameToWorkerLogicalName.keySet()) {
            if(_hostNameToWorkerLogicalName.get(key).equals(worker)) {
                return key;
            }
        }
            return "Unknown worker logical name";
    }

//    @Override
//    public void preStart() {
////        cluster.subscribe(getSelf(), ClusterEvent.MemberUp.class);
//    }
//
//    @Override
//    public void postStop() {
//        cluster.unsubscribe(getSelf());
//    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.UnreachableMember.class);
    }

    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof ClusterEvent.UnreachableMember) {
            ClusterEvent.UnreachableMember unreachableMember = (ClusterEvent.UnreachableMember) message;
            log(unreachableMember.member().address().toString() + " is unreachable!");

            for(String name: _nameToPath.keySet()) {
                if(_nameToPath.get(name).address().toString().equals(unreachableMember.member().address().toString())){
                    _nameToPath.remove(name);
                    log(_hostNameToWorkerLogicalName.get(name)+" is removed from the system.");
                    _hostNameToWorkerLogicalName.remove(name);
                } else {
//                    System.out.println(_nameToPath.get(name) + " != " + unreachableMember.member().address().toString());
                }

            }
//
//            System.out.println("_nameToPath: "+_nameToPath);
//            System.out.println("_nameToWorkerLogicalName: " + _hostNameToWorkerLogicalName);

        } else if(message instanceof WorkerRegistrationMessage) {
            WorkerRegistrationMessage workerRegistrationMessage = (WorkerRegistrationMessage)message;
            if(_nameToPath.containsKey(workerRegistrationMessage.getName()))
                log(workerRegistrationMessage.getName()+" is registered again! ");
//            _nameToActors.put(workerRegistrationMessage.getName(), getSender());
            _nameToPath.put(workerRegistrationMessage.getName(), getSender().path());
            final String ip = extractIpFromActorAddress(getSender().path().toString());
            final String logicalName = ip +":"+ workerRegistrationMessage.getPort();
            _hostNameToWorkerLogicalName.put(workerRegistrationMessage.getName(), logicalName);
            log("[" +  workerRegistrationMessage.getName() + "] is registered on " + _hostNameToWorkerLogicalName.get(workerRegistrationMessage.getName()));
            getSender().tell(new WorkerRegistrationResponseMessage(InetAddress.getLocalHost().getHostAddress(), ip, workerRegistrationMessage.getPort()), getSelf());
        } else if (message instanceof ElasticTaskRegistrationMessage) {
            ElasticTaskRegistrationMessage registrationMessage = (ElasticTaskRegistrationMessage) message;
            _taskidToActorName.put(registrationMessage.taskId, registrationMessage.hostName);
            _elasticTaskIdToWorker.put(registrationMessage.taskId, getWorkerLogicalName(registrationMessage.hostName));
            log("Task " + registrationMessage.taskId + " is launched on " + getWorkerLogicalName(registrationMessage.hostName) + ".");

        } else if (message instanceof RouteRegistrationMessage) {
            RouteRegistrationMessage registrationMessage = (RouteRegistrationMessage) message;
            for(int i: registrationMessage.routes) {
                if(!registrationMessage.unregister) {
                    _taskidRouteToWorker.put(registrationMessage.taskid + "." + i, getWorkerLogicalName(registrationMessage.host));
                    System.out.println("Route " + registrationMessage.taskid + "." + i + " is bound on " + getWorkerLogicalName(registrationMessage.host));
                } else {
                    _taskidRouteToWorker.remove(registrationMessage.taskid + "." + i);
                    System.out.println("Route " + registrationMessage.taskid + "." + i + " is removed from " + getWorkerLogicalName(registrationMessage.host));
                }
            }

        } else if (message instanceof String) {
            log("message received: " + message);
        } else if (message instanceof LogMessage) {
            LogMessage logMessage = (LogMessage) message;
            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            //get current date time with Date()
            Date date = new Date();
            log(getWorkerLogicalName(logMessage.host), logMessage.msg);
//            System.out.println(dateFormat.format(date)+"[" + getWorkerLogicalName(logMessage.host) + "] "+ logMessage.msg);
        } else if (message instanceof WorkerCPULoad) {
            WorkerCPULoad load = (WorkerCPULoad) message;
            ResourceManager.instance().updateWorkerCPULoad(getWorkerLogicalName(load.hostName), load);
        }
    }

    void log(String logger, String content) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println(dateFormat.format(date)+ "[" + logger + "] " + content);
    }

    void log(String content) {
        log("Master", content);
    }

    public static Master createActor() {
        try {
            final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                    .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLocalHost().getHostAddress()))
                    .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                    .withFallback(ConfigFactory.load()); 
            ActorSystem system = ActorSystem.create("ClusterSystem", config);

            system.actorOf(Props.create(Master.class), "master");
            Master ret = null;
            while(ret==null) {
                ret = Master.getInstance();
                Utils.sleep(100);
                System.out.println("Waiting Elastic Master to launch!");
            }
            return Master.getInstance();
        }
        catch (UnknownHostException e ) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        try{
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname="+ InetAddress.getLocalHost().getHostAddress()))
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                .withFallback(ConfigFactory.load());

        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Master.class), "master");
    }
    catch (UnknownHostException e ) {
        e.printStackTrace();
    }

    }

    @Override
    public List<String> getAllHostNames() throws TException {
//        return new ArrayList<>(_nameToActors.keySet());
        return new ArrayList<>(_nameToPath.keySet());
    }

    @Override
    public void migrateTasks(String originalHostName, String targetHostName, int taskId, int routeNo) throws MigrationException, TException {
        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(originalHostName)))
            throw new MigrationException("originalHostName " + originalHostName + " does not exists!");
        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(targetHostName)))
            throw new MigrationException("targetHostName " + targetHostName + " does not exists!");
        try {
//            _taskidToActorName.get(taskId);
            getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskId))).tell(new TaskMigrationCommand(getHostByWorkerLogicalName(originalHostName), getHostByWorkerLogicalName(targetHostName), taskId, routeNo), getSelf());
//            getContext().actorFor(_nameToPath.get(getHostByWorkerLogicalName(originalHostName))).tell(new TaskMigrationCommand(getHostByWorkerLogicalName(originalHostName),getHostByWorkerLogicalName(targetHostName),taskId,routeNo),getSelf());
    //        _nameToActors.get(getHostByWorkerLogicalName(originalHostName)).tell(new TaskMigrationCommand(getHostByWorkerLogicalName(originalHostName),getHostByWorkerLogicalName(targetHostName),taskId,routeNo),getSelf());
            log("[Elastic]: Migration message has been sent!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createRouting(String workerName, int taskid, int routeNo, String type) throws TException {
        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(workerName))) {
            throw new HostNotExistException("Host " + workerName + " does not exist!");
        }
        try {
            getContext().actorFor(_nameToPath.get(getHostByWorkerLogicalName(workerName))).tell(new RoutingCreatingCommand(taskid, routeNo, type), getSelf());
//          _nameToActors.get(getHostByWorkerLogicalName(workerName)).tell(new RoutingCreatingCommand(taskid, routeNo, type), getSelf());
            log("RoutingCreatingCommand has been sent!");
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void withdrawRemoteRoute(int taskid, int route) throws TException {
        if(!_taskidToActorName.containsKey(taskid)) {
            throw new TaskNotExistException("task "+ taskid + " does not exist!");
        }
        String hostName = _taskidToActorName.get(taskid);
        if(!_nameToPath.containsKey(hostName)) {
            throw new HostNotExistException("host " + hostName + " does not exist!");
        }
        RemoteRouteWithdrawCommand command = new RemoteRouteWithdrawCommand(getHostByWorkerLogicalName(hostName), taskid, route);
        try {
            getContext().actorFor(_nameToPath.get(hostName)).tell(command, getSelf());
            log("RemoteRouteWithdrawCommand has been sent to " + hostName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

//        _nameToActors.get(hostName).tell(command, getSelf());
//        System.out.println("RemoteRouteWithdrawCommand has been sent to " + hostName);

    }

    @Override
    public double reportTaskThroughput(int taskid) throws TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final Inbox inbox = Inbox.create(getContext().system());
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ThroughputQueryCommand(taskid));
//        inbox.send(_nameToActors.get(_taskidToActorName.get(taskid)), new ThroughputQueryCommand(taskid));
        return (double)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));

    }

    @Override
    public String queryDistribution(int taskid) throws TException {
        return getDistributionHistogram(taskid).toString();
    }


    @Override
    public String getLiveWorkers() throws TException {
        String ret = "";
        for(String name: _nameToPath.keySet()) {
            ret +=_hostNameToWorkerLogicalName.get(name) + ":" + name +"\n";
        }
        ret += "_hostNameToWorkerLogicalName" + _hostNameToWorkerLogicalName;
        return ret;
    }

    @Override
    public String queryRoutingTable(int taskid) throws TaskNotExistException, TException {
//        if(!_taskidToActorName.containsKey(taskid))
//            throw new TaskNotExistException("task " + taskid + " does not exist!");
//        final Inbox inbox = Inbox.create(getContext().system());
//        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new RoutingTableQueryCommand(taskid));
//        return (String)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));
        return getRoutingTable(taskid).toString();
    }

    @Override
    public void reassignBucketToRoute(int taskid, int bucket, int originalRoute, int newRoute) throws TaskNotExistException, TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        ReassignBucketToRouteCommand command = new ReassignBucketToRouteCommand(taskid, bucket, originalRoute, newRoute);
        final Inbox inbox = Inbox.create(getContext().system());

        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), command);
        inbox.receive(new FiniteDuration(2000, TimeUnit.SECONDS));

//        getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))).tell(command, getSelf());
    }

    @Override
    public String optimizeBucketToRoute(int taskid) throws TaskNotExistException, TException {
        try {
           return ElasticScheduler.getInstance().optimizeBucketToRoutingMapping(taskid);
        } catch (RoutingTypeNotSupportedException e) {
            e.printStackTrace();
            return e.getMessage();
        } catch (Exception ee) {
            ee.printStackTrace();
            return ee.getMessage();
        }
    }

    @Override
    public String subtaskLevelLoadBalancing(int taskid) throws TaskNotExistException, TException {
        try {
            return ElasticScheduler.getInstance().workerLevelLoadBalancing(taskid);
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    @Override
    public String queryWorkerLoad() throws TException {
        return ResourceManager.instance().printString();
    }

    @Override
    public String naiveWorkerLevelLoadBalancing(int taskid) throws TaskNotExistException, TException {
        if(!_elasticTaskIdToWorker.containsKey(taskid))
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        return ElasticScheduler.getInstance().naiveWorkerLevelLoadBalancing(taskid);
    }

    @Override
    public void logOnMaster(String from, String msg) throws TException {
        log(from, msg);
    }

    String extractIpFromActorAddress(String address) {
        Pattern p = Pattern.compile( "@([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)" );
        Matcher m = p.matcher(address);
        if(m.find()) {
            return m.group(1);
        } else {
            System.err.println("cannot extract valid ip from " + address);
            return null;
        }
    }

    public Histograms getDistributionHistogram(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final Inbox inbox = Inbox.create(getContext().system());
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new DistributionQueryCommand(taskid));
        return (Histograms)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));
    }

    public RoutingTable getRoutingTable(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final Inbox inbox = Inbox.create(getContext().system());
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new RoutingTableQueryCommand(taskid));
        return (RoutingTable)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));
    }

    public Histograms getBucketDistribution(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final Inbox inbox = Inbox.create(getContext().system());
        inbox.send(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new BucketDistributionQueryCommand(taskid));
        return (Histograms)inbox.receive(new FiniteDuration(30, TimeUnit.SECONDS));
    }

    public String getRouteHosterName(int taskid, int route) {
        return _taskidRouteToWorker.get(taskid + "." + route);
    }

//    public class MasterService extends backtype.storm.generated.MasterService.Iface {
//
//    }
}
