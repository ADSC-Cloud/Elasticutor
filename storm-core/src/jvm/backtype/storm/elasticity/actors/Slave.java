package backtype.storm.elasticity.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import backtype.storm.elasticity.ElasticTaskHolder;
import backtype.storm.elasticity.message.actormessage.*;
import backtype.storm.elasticity.message.actormessage.Status;
import backtype.storm.elasticity.resource.ResourceMonitor;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.utils.ConfigReader;
import backtype.storm.elasticity.utils.Histograms;
import backtype.storm.elasticity.utils.timer.SmartTimer;
import backtype.storm.generated.HostNotExistException;
import backtype.storm.generated.MasterService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import scala.concurrent.duration.FiniteDuration;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Robert on 11/11/15.
 */
public class Slave extends UntypedActor {

    Cluster cluster = Cluster.get(getContext().system());

//    Map<String, ActorRef> _nameToActors = new HashMap<>();

    Map<String, ActorPath> _nameToPath = new ConcurrentHashMap<>();

    String _name;

    String _logicalName;

    ActorSelection _master;

    static Slave _instance;

    int _port;

    String _ip;

    MasterService.Client thriftClient;

    final Object thriftClientLock = new Object();

    boolean supervisorActor = false; // indicate if this actor is used by a supervisor to communicate with the master.

    private Inbox _inbox;

    private Inbox getInbox() {
        if(_inbox == null) {
            _inbox = Inbox.create(getContext().system());
        }
        return _inbox;
    }

    void connectToMasterThriftServer(String ip, int port) {
        System.out.println("Thrift server ip: " + ip + " port: " + port);
        TTransport transport = new TSocket(ip, port);
        try {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);

            thriftClient = new MasterService.Client(protocol);
        } catch (TTransportException e) {
            e.printStackTrace();
        }

    }

    public Slave(String name, String port) {
//        _name = name+":"+port+"-"+ ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        _name = name + ":" + port;
        _port = Integer.parseInt(port);
        _instance = this;
        System.out.println("Slave constructor is called!");
//        createLiveNotificationHeartbeat();
    }

    public Slave(String name, String port, boolean supervisorActor) {
        this(name, port);
        this.supervisorActor = supervisorActor;
    }

    private void createLiveNotificationHeartbeat() {
        final int signiture = new Random().nextInt();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        Thread.sleep(10000);
                        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
//                        ;
                        sendMessageToMaster("I am still alive! " + signiture + "Memory usage: " + heapMemoryUsage.getUsed()/1024/1024 + "MB!");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static Slave getInstance() {
        return _instance;
    }

    public static Slave waitAndGetInstance() {
        try{
            while(_instance==null) {
                Thread.sleep(1000);
            }
            return _instance;
        } catch (InterruptedException e) {
            return _instance;
        }
    }

    @Override
    public void preStart() {
        cluster.subscribe(getSelf(), ClusterEvent.MemberUp.class, ClusterEvent.UnreachableMember.class);
    }

    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        try {
            if (message instanceof ClusterEvent.CurrentClusterState) {
                ClusterEvent.CurrentClusterState state = (ClusterEvent.CurrentClusterState) message;
                for (Member member : state.getMembers()) {
                    if (member.status().equals(MemberStatus.up())) {
                        register(member);
    //                    member.address().toString();
                    }
                }
            } else if (message instanceof ClusterEvent.MemberUp) {
                ClusterEvent.MemberUp memberUp = (ClusterEvent.MemberUp) message;
                register(memberUp.member());
            } else if (message instanceof ClusterEvent.UnreachableMember) {
                ClusterEvent.UnreachableMember unreachableMember = (ClusterEvent.UnreachableMember) message;
                System.out.println(unreachableMember.member().address().toString() + " is unreachable!");

                for(String name: _nameToPath.keySet()) {
                    if(_nameToPath.get(name).address().toString().equals(unreachableMember.member().address().toString())){
                        System.out.println(name+" is removed! "+ _nameToPath.get(name).address().toString() +"-->"+unreachableMember.member().address().toString());
                        _nameToPath.remove(name);
                        sendMessageToMaster(name+" is removed! ");
                    } else {
                        System.out.println(_nameToPath.get(name).address().toString() + " != " + unreachableMember.member().address().toString());
                    }

                }
            }else if (message instanceof WorkerRegistrationMessage) {
                WorkerRegistrationMessage workerRegistrationMessage = (WorkerRegistrationMessage) message;
                _nameToPath.put(workerRegistrationMessage.getName(), getSender().path());
                System.out.println("[Elastic]: I am connected with " + ((WorkerRegistrationMessage) message).getName() + "[" + getSender() + "]");
            } else if (message instanceof TaskMigrationCommand) {
                System.out.println("[Elastic]: received  TaskMigrationCommand!");
                TaskMigrationCommand taskMigrationCommand = (TaskMigrationCommand) message;
                handleTaskMigrationCommandMessage(taskMigrationCommand);
                getSender().tell("Task Migration finishes!", getSelf());
            } else if (message instanceof ElasticTaskMigrationMessage) {
//                sendMessageToMaster("Received Migration Message!!!!");
                handleElasticTaskMigrationMessage((ElasticTaskMigrationMessage) message);
            } else if (message instanceof RoutingCreatingCommand) {
                RoutingCreatingCommand creatingCommand = (RoutingCreatingCommand) message;
                handleRoutingCreatingCommand(creatingCommand);
                getSender().tell("Routing Creation Finished!", getSender());

            } else if (message instanceof RemoteRouteWithdrawCommand) {
                RemoteRouteWithdrawCommand withdrawCommand = (RemoteRouteWithdrawCommand) message;
                try {
                    handleWithdrawRemoteElasticTasks(withdrawCommand);
                    getSender().tell(Status.OK(), getSelf());
                } catch (Exception e) {
                    getSender().tell(Status.Error(e.getMessage()), getSelf());
                }
            } else if (message instanceof String) {
                System.out.println("I received message " + message);
                sendMessageToMaster("I received message " + message);
            } else if (message instanceof ThroughputQueryCommand) {
                ThroughputQueryCommand throughputQueryCommand = (ThroughputQueryCommand) message;
                double throughput = ElasticTaskHolder.instance().getThroughput(throughputQueryCommand.taskid);
                getSender().tell(throughput, getSelf());
            } else if (message instanceof DistributionQueryCommand) {
                DistributionQueryCommand distributionQueryCommand = (DistributionQueryCommand)message;
                Histograms distribution = ElasticTaskHolder.instance().getDistribution(distributionQueryCommand.taskid);
                getSender().tell(distribution, getSelf());
            } else if (message instanceof RoutingTableQueryCommand) {
                RoutingTableQueryCommand queryCommand = (RoutingTableQueryCommand)message;
                RoutingTable queryResult;
                if(queryCommand.completeRouting){
                    queryResult = ElasticTaskHolder.instance().getRoutingTable(queryCommand.taskid);
                } else {
                    queryResult = ElasticTaskHolder.instance().getOriginalRoutingTable(queryCommand.taskid);
                }
                getSender().tell(queryResult, getSelf());
            } else if (message instanceof ReassignBucketToRouteCommand) {
                System.out.println("I received ReassignBucketToRouteCommand message " + message);
                ReassignBucketToRouteCommand reassignBucketToRouteCommand = (ReassignBucketToRouteCommand) message;
                ElasticTaskHolder.instance().reassignHashBucketToRoute(reassignBucketToRouteCommand.taskId, reassignBucketToRouteCommand.bucketId,
                        reassignBucketToRouteCommand.originalRoute, reassignBucketToRouteCommand.newRoute);
                getSender().tell("Finished", getSelf());
            } else if (message instanceof BucketDistributionQueryCommand) {
                System.out.println("I received BucketDistributionQueryCommand!");

                BucketDistributionQueryCommand command = (BucketDistributionQueryCommand) message;
                getSender().tell(ElasticTaskHolder.instance().getBucketDistributionForBalancedRoutingTable(command.taskid), getSelf());
            } else if (message instanceof WorkerRegistrationResponseMessage) {
                WorkerRegistrationResponseMessage responseMessage = (WorkerRegistrationResponseMessage) message;
                System.out.println("Assigned logical name is" + responseMessage);
                _logicalName = responseMessage.toString();
                _ip = responseMessage.ip;
                connectToMasterThriftServer(responseMessage.masterIp, 9090);
            } else if (message instanceof TestAliveMessage) {
//                sendMessageToMaster("Alive: " + ((TestAliveMessage) message).msg);
            } else if (message instanceof ScalingOutSubtaskCommand) {
                System.out.println("ScalingOutSubtaskCommand response will be sent!");
                getSender().tell(ElasticTaskHolder.instance().handleScalingOutSubtaskCommand(((ScalingOutSubtaskCommand) message).taskId), getSelf());
                System.out.println("ScalingOutSubtaskCommand response is sent!");
            } else if (message instanceof ScalingInSubtaskCommand) {
                System.out.println("ScalingInSubtaskCommand response will be sent!");
                getSender().tell(ElasticTaskHolder.instance().handleScalingInSubtaskCommand(((ScalingInSubtaskCommand) message).taskId), getSelf());
//                getSender().tell(ElasticTaskHolder.instance().handleScalingInSubtaskCommand(((ScalingInSubtaskCommand) message).taskId), getSelf());
//                getSender().tell(ElasticTaskHolder.instance().handleScalingInSubtaskCommand(((ScalingInSubtaskCommand) message).taskId), getSelf());
                sendMessageObjectToMaster("ScalingInSubtaskCommand has been sent!");
                System.out.println("ScalingInSubtaskCommand response is sent!");
            } else if (message instanceof SubtaskLevelLoadBalancingCommand) {

                getSender().tell(ElasticTaskHolder.instance().handleSubtaskLevelLoadBalancingCommand(((SubtaskLevelLoadBalancingCommand) message).taskid), getSelf());
            }
            else  {
                System.out.println("[Elastic]: Unknown message.");
                unhandled(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
            sendMessageToMaster(e.getMessage());
        }
    }

    public void sendMessageToNode(String targetNode, Object object) throws HostNotExistException {
        if(!_nameToPath.containsKey(targetNode)) {
            throw new HostNotExistException(targetNode +" does not exist");
        }
        getContext().actorFor(_nameToPath.get(targetNode)).tell(object, getSelf());
    }

    public Object sendMessageToNodeAndWaitForResponse(String targetNode, Object object) throws HostNotExistException {
        if(!_nameToPath.containsKey(targetNode)) {
            throw new HostNotExistException(targetNode + " does not exist!");
        }
//        final Inbox inbox = Inbox.create(getContext().system());
        final Inbox inbox = getInbox();
//        sendMessageToMaster("Begin to send " + object + " to " + targetNode);
        inbox.send(getContext().actorFor(_nameToPath.get(targetNode)), object);
        try {
            return inbox.receive(new FiniteDuration(3000, TimeUnit.SECONDS));
        } catch (TimeoutException e) {
            e.printStackTrace();
            return null;
        }
    }

    ElasticTaskMigrationMessage addIpInfo(ElasticTaskMigrationMessage message, String address) {
        message._ip = extractIpFromActorAddress(address);
        return message;
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

    void register(Member member) {
        if(member.hasRole("master")) {
            _master = getContext().actorSelection(member.address()+"/user/master");

            int numberOfCores = ResourceMonitor.getNumberOfProcessors();
            if(backtype.storm.elasticity.config.Config.CPUBudget > 0) {
                numberOfCores = Math.min(backtype.storm.elasticity.config.Config.CPUBudget, numberOfCores);
            }

            if(supervisorActor) {
                _master.tell(new SupervisorRegistrationMessage(_name, _port, numberOfCores),getSelf());
            } else {
                _master.tell(new WorkerRegistrationMessage(_name, _port, numberOfCores),getSelf());
            }

            System.out.println("I have sent registration message to master.");
        } else if (member.hasRole("slave")) {
            if(!supervisorActor) {
                getContext().actorSelection(member.address()+"/user/slave")
                        .tell(new WorkerRegistrationMessage(_name, _port),getSelf());
                System.out.format("I have sent registration message to %s\n", member.address());
            }
//            sendMessageToMaster("I have sent registration message to "+ member.address());
        }
    }

    public void sendMessageToMaster(String message) {
//        _master.tell(new LogMessage(message, _name ), getSelf());
        try {
            if(message != null)
            synchronized (thriftClientLock) {
                thriftClient.logOnMaster(_logicalName, message);
            }
        } catch (Exception e) {
            System.out.println("thriftClient: " + thriftClient + " _logicalName: " + _logicalName + " message: " + message );
            e.printStackTrace();
        }
    }

    public void reportStateMigrationToMaster(long stateSize) {
        _master.tell(new StateMigrationReportMessage(stateSize), getSender());
    }


    public void logOnMaster(String message) {
        try {
            if(message != null)
            synchronized (thriftClientLock) {

            thriftClient.logOnMaster(_logicalName, message);
            }
        } catch (Exception e) {
            System.out.println("thriftClient: " + thriftClient + " _logicalName: " + _logicalName + " message: " + message );
            e.printStackTrace();
        }
    }

    public void registerOriginalElasticTaskToMaster(int taskId) {
        _master.tell(new ElasticTaskRegistrationMessage(taskId, _name),getSelf());
    }

    private void handleTaskMigrationCommandMessage(TaskMigrationCommand taskMigrationCommand) {

        if(!_nameToPath.containsKey(taskMigrationCommand._targetHostName)) {
            System.out.println("[Elastic]:target host "+ taskMigrationCommand._targetHostName+" does not exist!");
            sendMessageToMaster(taskMigrationCommand._targetHostName+" does not exist! Valid names are "+_nameToPath.keySet());
            return;
        }

//        if(!_nameToPath.containsKey(taskMigrationCommand._originalHostName)) {
//            System.out.println("[Elastic]:target host "+ taskMigrationCommand._targetHostName+" does not exist!");
//            sendMessageToMaster(taskMigrationCommand._originalHostName+" does not exist! Valid names are "+_nameToPath.keySet());
//            return;
//        }
        try{
//            ElasticTaskHolder.instance().migrateSubtaskToRemoteHost(taskMigrationCommand._targetHostName, taskMigrationCommand._taskID, taskMigrationCommand._route);
            ElasticTaskHolder.instance().migrateSubtask(taskMigrationCommand._targetHostName, taskMigrationCommand._taskID, taskMigrationCommand._route);
        } catch (Exception e) {
            sendMessageToMaster(e.getMessage());
        }
    }

    private void handleElasticTaskMigrationMessage(ElasticTaskMigrationMessage elasticTaskMigrationMessage) {
        System.out.println("[Elastic]: received elastic mask migration message from" + getSender());
        SmartTimer.getInstance().start("SubtaskMigrateTarget", "launch");
        ElasticTaskMigrationConfirmMessage confirmMessage = ElasticTaskHolder.instance().handleGuestElasticTasks(addIpInfo(elasticTaskMigrationMessage,getSender().path().toString()));

//        registerRoutesOnMaster(elasticTaskMigrationMessage._elasticTask.get_taskID(), elasticTaskMigrationMessage._elasticTask.get_routingTable().getRoutes());

        if(confirmMessage!=null) {
            SmartTimer.getInstance().stop("SubtaskMigrateTarget", "launch");
            sendMessageToMaster(SmartTimer.getInstance().getTimerString("SubtaskMigrateTarget"));
            getSender().tell(confirmMessage, getSelf());
            sendMessageToMaster("I have handled the mask migration message");
        } else {
            System.err.println("Failed to deploy remote elastic tasks!");
            _master.tell("Failed to deploy elastic tasks", null);
        }
    }

    private void handleRoutingCreatingCommand(RoutingCreatingCommand creatingCommand) {
        try {
            sendMessageToMaster("begin to handle crate routing command!");
            ElasticTaskHolder.instance().createRouting(creatingCommand._task, creatingCommand._numberOfRoutes, creatingCommand._routingType);

        } catch (Exception e) {
            sendMessageToMaster(e.getMessage());
        }
    }

    private void handleWithdrawRemoteElasticTasks(RemoteRouteWithdrawCommand withdrawCommand) throws Exception {
        ElasticTaskHolder.instance().withdrawRemoteElasticTasks( withdrawCommand.taskId, withdrawCommand.route);
    }

    static public Slave createActor(String name, String port) {

        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0")
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + backtype.storm.elasticity.config.Config.slaveIp))
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
                .withFallback(ConfigFactory.parseString("akka.cluster.seed-nodes = [ \"akka.tcp://ClusterSystem@"+ backtype.storm.elasticity.config.Config.masterIp + ":2551\"]"))
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.maximum-frame-size = 134217728"))
                .withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("ClusterSystem", config);
        system.actorOf(Props.create(Slave.class, name, port), "slave");

        System.out.println("Slave actor is created on " + backtype.storm.elasticity.config.Config.slaveIp);

        return Slave.waitAndGetInstance();

    }

    static public Slave createActor(String name, String port, boolean supervisorActor) {

        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0")
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + backtype.storm.elasticity.config.Config.slaveIp))
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
                .withFallback(ConfigFactory.parseString("akka.cluster.seed-nodes = [ \"akka.tcp://ClusterSystem@"+ backtype.storm.elasticity.config.Config.masterIp + ":2551\"]"))
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.maximum-frame-size = 134217728"))
                .withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("ClusterSystem", config);
        system.actorOf(Props.create(Slave.class, name, port, supervisorActor), "slave");

        System.out.println("Slave actor is created on " + backtype.storm.elasticity.config.Config.slaveIp);

        return Slave.waitAndGetInstance();

    }

    public static void main(String[] args) {
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=0")
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [slave]"))
                .withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("ClusterSystem", config);

        system.actorOf(Props.create(Slave.class, args[0]), "slave");

    }

    public void registerRoutesOnMaster(int taskid, int route) {
        ArrayList<Integer> routes = new ArrayList<>();
        routes.add(route);
        registerRoutesOnMaster(taskid, routes);
    }

    public void registerRoutesOnMaster(int taskid, ArrayList<Integer> routes) {
        RouteRegistrationMessage registrationMessage = new RouteRegistrationMessage(taskid, routes, _name);
        _master.tell(registrationMessage, getSelf());
    }

    public void unregisterRoutesOnMaster(int taskid, ArrayList<Integer> routes) {
        RouteRegistrationMessage registrationMessage = new RouteRegistrationMessage(taskid, routes, _name);
        registrationMessage.setUnregister();
        _master.tell(registrationMessage, getSelf());
    }

    public void unregisterRoutesOnMaster(int taskid, int route) {
        ArrayList<Integer> routes = new ArrayList<>();
        routes.add(route);
        unregisterRoutesOnMaster(taskid, routes);
    }

    public void reportWorkerCPULoadToMaster(double workerLoad) {
        reportWorkerCPULoadToMaster(workerLoad, -1.0);
    }

    public void reportWorkerCPULoadToMaster(double workerLoad, double systemLoad) {
        WorkerCPULoad workerCPULoad = new WorkerCPULoad(_name, workerLoad, systemLoad);
        _master.tell(workerCPULoad, getSelf());
    }

    public String getLogicalName() {
        return _logicalName;
    }

    public String getName() {
        return _name;
    }

    public String getIp() { return _ip;}

    public void sendMessageObjectToMaster(Object message) {
        _master.tell(message, getSelf());
    }

}
