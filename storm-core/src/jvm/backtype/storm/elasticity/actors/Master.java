package backtype.storm.elasticity.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import backtype.storm.elasticity.exceptions.RoutingTypeNotSupportedException;
import backtype.storm.elasticity.message.actormessage.*;
import backtype.storm.elasticity.message.actormessage.Status;
import backtype.storm.elasticity.resource.ResourceManager;
import backtype.storm.elasticity.routing.RoutingTable;
import backtype.storm.elasticity.routing.RoutingTableUtils;
import backtype.storm.elasticity.scheduler.ElasticExecutorInfo;
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
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Robert on 11/11/15.
 */
public class Master extends UntypedActor implements MasterService.Iface {

    Cluster cluster = Cluster.get(getContext().system());

    private Map<String, ActorPath> _nameToPath = new ConcurrentHashMap<>();

    private Map<Integer, String> _taskidToActorName = new ConcurrentHashMap<>();

    public Map<Integer, String> _elasticTaskIdToWorkerLogicalName = new HashMap<>();

    public Map<String, String> _taskidRouteToWorker = new ConcurrentHashMap<>();

    private Map<String, String> _hostNameToWorkerLogicalName = new HashMap<>();

    private Map<String, Set<String>> _ipToWorkerLogicalName = new HashMap<>();

    private Set<String> _supervisorActorNames = new HashSet<>();

    private Map<Integer, List<String>> _taskToCoreLocations = new HashMap<>();

    private BlockingQueue<Inbox> inboxes = new LinkedBlockingQueue<>();

    private Inbox prioritizedInbox = Inbox.create(getContext().system());

    static Master _instance;

    public static Master getInstance() {
        return _instance;
    }

    public Master() {
        _instance = this;
        createThriftServiceThread();
        createInboxes();
    }

    private void createInboxes() {
        try {
            for (int i = 0; i < 10; i++) {
                inboxes.put(Inbox.create(getContext().system()));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Inbox getOneInbox() throws InterruptedException {
        return inboxes.take();
    }

    private void returnInbox(Inbox inbox) throws InterruptedException {
        inboxes.put(inbox);
    }

    private synchronized Object sendAndReceiveWithPriority(ActorRef ref, Object message) throws TimeoutException, InterruptedException {
        return sendAndReceiveWithPriority(ref, message, 3000, TimeUnit.SECONDS);
    }

    private synchronized Object sendAndReceiveWithPriority(ActorRef ref, Object message, int timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        prioritizedInbox.send(ref, message);
        return prioritizedInbox.receive(new FiniteDuration(timeout, timeUnit));
    }

    private synchronized Object sendAndReceive(ActorRef ref, Object message) throws TimeoutException, InterruptedException {
        return sendAndReceive(ref, message, 3000, TimeUnit.SECONDS);
    }

    private synchronized Object sendAndReceive(ActorRef ref, Object message, int timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
        final Inbox inbox = getOneInbox();
        inbox.send(ref, message);
        final Object ret = inbox.receive(new FiniteDuration(timeout, timeUnit));
        returnInbox(inbox);
        return ret;
    }

    public void createThriftServiceThread() {

//        System.out.println("thrift Service Thread is called!");
//        System.out.println(Thread.currentThread().getStackTrace());
//        Thread.dumpStack();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    log("Begin to create thrift daemon thread...");
                    MasterService.Processor processor = new MasterService.Processor(_instance);
                    TServerTransport serverTransport = new TServerSocket(9090);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

                    log("Starting the monitoring daemon...");
                    server.serve();
                } catch (Exception e) {
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

            if(_supervisorActorNames.contains(unreachableMember.member().address().toString())) {
                final String ip = extractIpFromActorAddress(unreachableMember.member().address().toString());
                System.out.println(String.format("Supervisor on %s is dead!", ip));
                _supervisorActorNames.remove(unreachableMember.member().address().toString());
                ResourceManager.instance().computationResource.unregisterNode(ip);
                System.out.println("Resource Manager is updated upon the dead of the supervisor.");
                return;
            }

            for(String name: _nameToPath.keySet()) {
                if(_nameToPath.get(name).address().toString().equals(unreachableMember.member().address().toString())){
                    final String ip = extractIpFromActorAddress(unreachableMember.member().address().toString());
                    if(ip == null) {
                        continue;
                    }
                    final String logicalName = _hostNameToWorkerLogicalName.get(name);

                    _ipToWorkerLogicalName.get(ip).remove(logicalName);
                    System.out.println(ip + " is removed!");
                    printIpToWorkerLogicalName();
//                    if(_ipToWorkerLogicalName.get(ip).isEmpty()) {
//                        _ipToWorkerLogicalName.remove(ip);
////                        ResourceManager.instance().computationResource.unregisterNode(ip);
//                    }
                    for(int task: _taskidToActorName.keySet()) {

                        if(_nameToPath.get(_taskidToActorName.get(task)).address().toString().equals(unreachableMember.member().address().toString())) {
                            System.out.println("_taskidToActorName--> Task: " + task + " + _taskidToActorName: " + _taskidToActorName.get(task));
//                            int i = 1;
//                            if(_taskToCoreLocations.get(task)!=null) {
//                                for (String coreIp : _taskToCoreLocations.get(task)) {
//                                    System.out.println("No: " + i++);
//                                    ResourceManager.instance().computationResource.returnProcessor(coreIp);
//                                    System.out.println("The CPU core for task " + task + " is returned, as the its hosting worker is dead!");
//                                }
//                            }
//                            for(String core: ElasticScheduler.getInstance().getElasticExecutorStatusManager().getAllocatedCoresForATask(task)) {
//                                ResourceManager.instance().computationResource.returnProcessor(core);
//                            }
                            ElasticScheduler.getInstance().unregisterElasticExecutor(task);

                            _elasticTaskIdToWorkerLogicalName.remove(task);
                            _taskidToActorName.remove(task);
                            _taskToCoreLocations.remove(task);
                            for(String str: _taskidRouteToWorker.keySet()) {
                                if(str.split("\\.")[0].equals(""+task)) {
                                    _taskidRouteToWorker.remove(str);
                                }
                            }
                        }
                    }

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

        } else if(message instanceof SupervisorRegistrationMessage){
            SupervisorRegistrationMessage supervisorRegistrationMessage = (SupervisorRegistrationMessage)message;
            final String ip = extractIpFromActorAddress(getSender().path().toString());
            ResourceManager.instance().computationResource.registerNode(ip, supervisorRegistrationMessage.getNumberOfProcessors());
            _supervisorActorNames.add(getSender().path().address().toString());
            System.out.println(String.format("Supervisor is registered on %s with %d processors", ip, supervisorRegistrationMessage.getNumberOfProcessors()));

        } else if(message instanceof WorkerRegistrationMessage) {
            WorkerRegistrationMessage workerRegistrationMessage = (WorkerRegistrationMessage)message;
            if(_nameToPath.containsKey(workerRegistrationMessage.getName()))
                log(workerRegistrationMessage.getName()+" is registered again! ");
//            _nameToActors.put(workerRegistrationMessage.getName(), getSender());
            _nameToPath.put(workerRegistrationMessage.getName(), getSender().path());
            final String ip = extractIpFromActorAddress(getSender().path().toString());
            if(ip == null) {
                System.err.println("WorkerRegistrationMessage is ignored, as we cannot extract a valid ip!");
                return;
            }
            final String logicalName = ip +":"+ workerRegistrationMessage.getPort();
            _hostNameToWorkerLogicalName.put(workerRegistrationMessage.getName(), logicalName);

            if(!_ipToWorkerLogicalName.containsKey(ip))
                _ipToWorkerLogicalName.put(ip, new HashSet<String>());
            _ipToWorkerLogicalName.get(ip).add(logicalName);

            printIpToWorkerLogicalName();
//            ResourceManager.instance().computationResource.registerNode(ip, workerRegistrationMessage.getNumberOfProcessors());
            System.out.println(ResourceManager.instance().computationResource);

            log("[" + workerRegistrationMessage.getName() + "] is registered on " + _hostNameToWorkerLogicalName.get(workerRegistrationMessage.getName()));
            getSender().tell(new WorkerRegistrationResponseMessage(InetAddress.getLocalHost().getHostAddress(), ip, workerRegistrationMessage.getPort()), getSelf());
        } else if (message instanceof ElasticTaskRegistrationMessage) {
            ElasticTaskRegistrationMessage registrationMessage = (ElasticTaskRegistrationMessage) message;
            _taskidToActorName.put(registrationMessage.taskId, registrationMessage.hostName);
            _elasticTaskIdToWorkerLogicalName.put(registrationMessage.taskId, getWorkerLogicalName(registrationMessage.hostName));

            final String ip = extractIpFromActorAddress(getSender().path().toString());
            ElasticScheduler.getInstance().registerElasticExecutor(registrationMessage.taskId, ip);

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
//                for(String n: _taskidRouteToWorker.keySet()) {
//                    System.out.println(String.format("%s: %s", n, _taskidRouteToWorker.get(n)));
//                }
            }

        } else if (message instanceof String) {
            log("message received: " + message);
        } else if (message instanceof LogMessage) {
            LogMessage logMessage = (LogMessage) message;
            //get current date time with Date()
            log(getWorkerLogicalName(logMessage.host), logMessage.msg);
//            System.out.println(dateFormat.format(date)+"[" + getWorkerLogicalName(logMessage.host) + "] "+ logMessage.msg);
        } else if (message instanceof WorkerCPULoad) {
            WorkerCPULoad load = (WorkerCPULoad) message;
            ResourceManager.instance().updateWorkerCPULoad(getWorkerLogicalName(load.hostName), load);
        } else if (message instanceof ExecutorScalingInRequestMessage) {
            ExecutorScalingInRequestMessage requestMessage = (ExecutorScalingInRequestMessage)message;
            ElasticScheduler.getInstance().addScalingRequest(requestMessage);
//            handleExecutorScalingInRequest(requestMessage.taskID);
        } else if (message instanceof ExecutorScalingOutRequestMessage) {
            ExecutorScalingOutRequestMessage requestMessage = (ExecutorScalingOutRequestMessage) message;
//            handleExecutorScalingOutRequest(requestMessage.taskId);
            ElasticScheduler.getInstance().addScalingRequest(requestMessage);
        } else if (message instanceof ElasticExecutorMetricsReportMessage) {
            ElasticExecutorMetricsReportMessage elasticExecutorMetricsReportMessage = (ElasticExecutorMetricsReportMessage) message;
            ElasticScheduler.getInstance().getElasticExecutorStatusManager().updateStateSize(elasticExecutorMetricsReportMessage.taskID, elasticExecutorMetricsReportMessage.stateSize);
            ElasticScheduler.getInstance().getElasticExecutorStatusManager().updateDataIntensivenessFactor(elasticExecutorMetricsReportMessage.taskID, elasticExecutorMetricsReportMessage.dataTransferBytesPerSecond);
        } else if (message instanceof DesirableParallelismMessage) {
            DesirableParallelismMessage desirableParallelismMessage = (DesirableParallelismMessage) message;
            ElasticScheduler.getInstance().getElasticExecutorStatusManager().updateDesirableParallelism(desirableParallelismMessage.taskID, desirableParallelismMessage.desriableParallelism);
        } else if (message instanceof StateMigrationReportMessage) {
            StateMigrationReportMessage reportMessage = (StateMigrationReportMessage) message;
            ElasticScheduler.getInstance().getMetaDataManager().reportStateMigration(reportMessage.stateSize);
        } else if (message instanceof IntraExecutorDataTransferReportMessage) {
            IntraExecutorDataTransferReportMessage dataTransferReportMessage = (IntraExecutorDataTransferReportMessage) message;
            ElasticScheduler.getInstance().getMetaDataManager().reportIntraExecutorDataTransfer(dataTransferReportMessage.dataTransferSize);
        }
    }

    public void handleExecutorScalingInRequest(int taskId) {
        try {
            RoutingTable routingTable = getRoutingTable(taskId);
            int targetRouteId = routingTable.getNumberOfRoutes() - 1;
            String hostWorkerLogicalName = _taskidRouteToWorker.get(taskId+"."+targetRouteId);

            if(hostWorkerLogicalName==null) {
                System.out.println("hostWorkerLogicalName is null!");
                System.out.println(taskId+"."+targetRouteId);
                for(String n: _taskidRouteToWorker.keySet()) {
                    System.out.println(String.format("%s: %s", n, _taskidRouteToWorker.get(n)));
                }
            }


            String hostIp = getIpForWorkerLogicalName(hostWorkerLogicalName);
            System.out.println("ScalingInSubtask will be called!");
            if(sendScalingInSubtaskCommand(taskId)) {
                System.out.println("Task" + taskId + " successfully scales in!");
                ResourceManager.instance().computationResource.returnProcessor(hostIp);
                ElasticScheduler.getInstance().getElasticExecutorStatusManager().scalingInExecutor(taskId);
            } else {
                System.out.println("Task " + taskId + " scaling in fails!");
            }


//            System.out.println("Current Routing Table: ");
//            System.out.println(getOriginalRoutingTable(taskId));
//            System.out.println("=====================================\n");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public String getAWorkerLogicalNameOnAGivenIp(String ip) {
        Set<String> workers = _ipToWorkerLogicalName.get(ip);
        return new ArrayList<>(workers).get(new Random().nextInt(workers.size()));
    }


    public String getIpForWorkerLogicalName(String workerLogicalName) {
        String hostIp = null;
        for(String ip : _ipToWorkerLogicalName.keySet()) {
            if(_ipToWorkerLogicalName.get(ip).contains(workerLogicalName)) {
                hostIp = ip;
                break;
            }
        }
        if(hostIp == null) {
            System.out.println(String.format("Cannot find ip for %s", workerLogicalName));
            System.out.println("_ipToWorkerLogicalName:");
            for(String ip: _ipToWorkerLogicalName.keySet()) {
                System.out.println(String.format("%s : %s", ip, _ipToWorkerLogicalName.get(ip)));
            }
        }
        return hostIp;

    }


    private void executorScalingOut(int taskid, String targetIp) throws TException {

//        if(!ResourceManager.instance().computationResource.allocateProcessOnPreferredNode(targetIp).equals(targetIp)) {
//            System.out.println("Cannot get cores from " + targetIp);
//            return;
//        }

        String workerHostName = _elasticTaskIdToWorkerLogicalName.get(taskid);
        String hostIP = getIpForWorkerLogicalName(workerHostName);
        RoutingTable balancecHashRouting = RoutingTableUtils.getBalancecHashRouting(getRoutingTable(taskid));
        if(balancecHashRouting == null) {
            createRouting(workerHostName,taskid,1,"balanced_hash");
            balancecHashRouting = RoutingTableUtils.getBalancecHashRouting(getRoutingTable(taskid));
        }

        System.out.println("ScalingOutSubtask of " + taskid + " will be called!");
        sendScalingOutSubtaskCommand(taskid);
        System.out.println("ScalingOutSubtask of " + taskid + "  is called!");
//            System.out.println("A local new task " + taskid + "." + balancecHashRouting.getNumberOfRoutes() +" is created!");
//            for(String name: _ipToWorkerLogicalName.keySet()) {
//                System.out.println(String.format("%s: %s", name, _ipToWorkerLogicalName.get(name)));
//            }
        ElasticScheduler.getInstance().getElasticExecutorStatusManager().scalingOutExecutor(taskid, hostIP);

        if(!targetIp.equals(hostIP)) {
//            Set<String> candidateHosterWorkers = _ipToWorkerLogicalName.get(targetIp);
//
//            List<String> candidates = new ArrayList<>();
//            candidates.addAll(candidateHosterWorkers);
//            String hosterWorker = candidates.get(new Random().nextInt(candidateHosterWorkers.size()));

            int targetRoute = getRoutingTable(taskid).getNumberOfRoutes() - 1 ;

            System.out.println("a local task " + taskid + "." + targetRoute+" will be migrated from " + workerHostName + " to " + targetIp);
            migrateTasks(targetIp, taskid, targetRoute);
            ElasticScheduler.getInstance().getElasticExecutorStatusManager().migrateRoute(taskid, targetRoute, targetIp);
            System.out.println("Task " + taskid + "." + targetRoute + " has been migrated!");
        }
    }


    public void handleExecutorScalingOutRequest(int taskid) {
        handleExecutorScalingOutRequest(taskid, null);
    }

    public void handleExecutorScalingOutRequest(int taskid, String preferredIp) {
        String targetIp = null;
        try {
            String workerHostName = _elasticTaskIdToWorkerLogicalName.get(taskid);
            if(preferredIp == null) {
                preferredIp = getIpForWorkerLogicalName(workerHostName);
            }

            targetIp = ResourceManager.instance().computationResource.allocateProcessOnPreferredNode(preferredIp);

            if(targetIp == null) {
                System.err.println("There is not enough computation resources for scaling out!");
                return;
            }

            if(!_taskToCoreLocations.containsKey(taskid)) {
                _taskToCoreLocations.put(taskid, new ArrayList<String>());
            }
            _taskToCoreLocations.get(taskid).add(targetIp);

            executorScalingOut(taskid, targetIp);
//            RoutingTable balancecHashRouting = RoutingTableUtils.getBalancecHashRouting(getRoutingTable(taskid));
//            if(balancecHashRouting == null) {
//                createRouting(workerHostName,taskid,1,"balanced_hash");
//                balancecHashRouting = RoutingTableUtils.getBalancecHashRouting(getRoutingTable(taskid));
//            }
//
//            System.out.println("ScalingOutSubtask will be called!");
//            sendScalingOutSubtaskCommand(taskid);
//            System.out.println("ScalingOutSubtask is called!");
////            System.out.println("A local new task " + taskid + "." + balancecHashRouting.getNumberOfRoutes() +" is created!");
////            for(String name: _ipToWorkerLogicalName.keySet()) {
////                System.out.println(String.format("%s: %s", name, _ipToWorkerLogicalName.get(name)));
////            }
//            ElasticScheduler.getInstance().getElasticExecutorStatusManager().scalingOutExecutor(taskid, preferredIp);
//
//            if(!hostIp.equals(preferredIp)) {
//                Set<String> candidateHosterWorkers = _ipToWorkerLogicalName.get(hostIp);
//
//                List<String> candidates = new ArrayList<>();
//                candidates.addAll(candidateHosterWorkers);
//                String hosterWorker = candidates.get(new Random().nextInt(candidateHosterWorkers.size()));
//
//                int targetRoute = getRoutingTable(taskid).getNumberOfRoutes() - 1 ;
//
//                System.out.println("a local task " + taskid + "." + targetRoute+" will be migrated from " + workerHostName + " to " + hosterWorker);
//                migrateTasks(workerHostName, hosterWorker, taskid, targetRoute);
//                System.out.println("Task " + taskid + "." + targetRoute + " has been migrated!");
//            }

//            System.out.println("Current Routing Table: ");
//            System.out.println(getOriginalRoutingTable(taskid));
//            System.out.println("=====================================\n");

        } catch (Exception e) {
            e.printStackTrace();
            if(targetIp != null) {
                ResourceManager.instance().computationResource.returnProcessor(targetIp);
            }
            System.out.println("HostIP: " + targetIp);
            for(String name: _ipToWorkerLogicalName.keySet()) {
                System.out.println(String.format("%s: %s", name, _ipToWorkerLogicalName.get(name)));
            }
        }

    }

    void log(String logger, String content) {
        DateFormat dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();
        System.out.println(dateFormat.format(date)+ " [" + logger + "]: " + content);
    }

    void log(String content) {
        log("Master", content);
    }

    public static Master createActor() {
            final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                    .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + backtype.storm.elasticity.config.Config.masterIp))
                    .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                    .withFallback(ConfigFactory.parseString("akka.cluster.seed-nodes = [ \"akka.tcp://ClusterSystem@"+ backtype.storm.elasticity.config.Config.masterIp + ":2551\"]"))
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

    public static void main(String[] args) {
        try{
        final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=2551")
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname="+ InetAddress.getLocalHost().getHostAddress()))
                .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
                .withFallback(ConfigFactory.parseString("akka.cluster.seed-nodes = [ \"akka.tcp://ClusterSystem@"+ backtype.storm.elasticity.config.Config.masterIp + ":2551\"]"))
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

    public void resourceAwareMigrateTask(String targetHostIP, int taskId, int routeNO) {
        if(!ResourceManager.instance().computationResource.allocateProcessOnGivenNode(targetHostIP))
            return;
        try {
            migrateTasks(targetHostIP, taskId, routeNO);
            String deallocatedCore = ElasticScheduler.getInstance().getElasticExecutorStatusManager().getAllocatedCoreForARoute(taskId, routeNO);
            ResourceManager.instance().computationResource.returnProcessor(deallocatedCore);
            ElasticScheduler.getInstance().getElasticExecutorStatusManager().migrateRoute(taskId, routeNO, targetHostIP);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void migrateTasks(String targetHostIP, int taskId, int routeNo) throws MigrationException, TException {
//        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(originalHostName)))
//            throw new MigrationException("originalHostName " + originalHostName + " does not exists!");
        final String targetWorkerLogicalName = Master.getInstance().getAWorkerLogicalNameOnAGivenIp(targetHostIP);


        if(!_nameToPath.containsKey(getHostByWorkerLogicalName(targetWorkerLogicalName)))
            throw new MigrationException("targetHostName " + targetWorkerLogicalName + " does not exists!");
        try {
//            getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskId))).tell(new TaskMigrationCommand(getHostByWorkerLogicalName(originalHostName), getHostByWorkerLogicalName(targetHostName), taskId, routeNo), getSelf());
//            log("[Elastic]: Migration message has been sent!");

//            final Inbox inbox = Inbox.create(getContext().system());
//            final Inbox inbox = getInbox();
            log("[Elastic]: Migration message has been sent!");
            sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskId))), new TaskMigrationCommand(getHostByWorkerLogicalName(targetWorkerLogicalName), taskId, routeNo),10000, TimeUnit.SECONDS);
//            inbox.receive(new FiniteDuration(10000, TimeUnit.SECONDS));

            ElasticScheduler.getInstance().getElasticExecutorStatusManager().migrateRoute(taskId, routeNo, extractIpFromWorkerLogicalName(targetWorkerLogicalName));
            return;

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
            sendAndReceive(getContext().actorFor(_nameToPath.get(getHostByWorkerLogicalName(workerName))), new RoutingCreatingCommand(taskid, routeNo, type), 10000, TimeUnit.SECONDS);
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
            log("RemoteRouteWithdrawCommand has been sent to " + hostName);
            Status status = (Status) sendAndReceive(getContext().actorFor(_nameToPath.get(hostName)), command, 1000, TimeUnit.SECONDS);
            if(status.code == Status.ERROR) {
                System.err.println(status.msg);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public double reportTaskThroughput(int taskid) throws TException {
        log("Received throughput query for task " + taskid);
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        final double ret;
        try {
            ret = (double)sendAndReceiveWithPriority(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ThroughputQueryCommand(taskid), 30000, TimeUnit.SECONDS);
//            ret = (double)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ThroughputQueryCommand(taskid), 300000, TimeUnit.SECONDS);
            log("answered " + taskid);
            return ret;
        } catch (Exception e) {
            System.out.print("[DEBUG:]");
            e.printStackTrace();
            return -1.0;
        }

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
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        try {
            sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new RoutingTableQueryCommand(taskid), 30, TimeUnit.SECONDS);
            return getRoutingTable(taskid).toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed!";
        }
    }

    @Override
    public void reassignBucketToRoute(int taskid, int bucket, int originalRoute, int newRoute) throws TaskNotExistException, TException {
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        ReassignBucketToRouteCommand command = new ReassignBucketToRouteCommand(taskid, bucket, originalRoute, newRoute);
        long startTime = System.currentTimeMillis();

        try {
            sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), command, 10000, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

//        System.out.println("Shard reassignment time: " + (System.currentTimeMillis() - startTime));

//        System.out.println("======================= End SHARD REASSIGNMENT =======================\n");
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
    public String optimizeBucketToRouteWithThreshold(int taskid, double theshold) throws TaskNotExistException, TException {
        try {
           return ElasticScheduler.getInstance().optimizeBucketToRoutingMapping(taskid, theshold);
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
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
        try {
            return (String)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new SubtaskLevelLoadBalancingCommand(taskid));
        } catch (Exception e) {
            e.printStackTrace();
            return "Timeout!";
        }
    }

    @Override
    public String workerLevelLoadBalancing(int taskid) throws TaskNotExistException, TException {
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
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid))
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        return ElasticScheduler.getInstance().naiveWorkerLevelLoadBalancing(taskid);
    }

    public void sendScalingOutSubtaskCommand(int taskid) throws TaskNotExistException, TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
        System.out.println("Scaling out message has been sent!");
        try {

            Status returnStatus = (Status)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ScalingOutSubtaskCommand(taskid), 30000, TimeUnit.SECONDS);

            if(returnStatus.code == Status.ERROR) {
                System.err.println(returnStatus.msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Scaling out response is received!");
    }

    @Override
    public void scalingOutSubtask(int taskid) throws TaskNotExistException, TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
        System.out.println("Scaling out message has been sent!");
        try {

            handleExecutorScalingOutRequest(taskid);
//            Status returnStatus = (Status)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ScalingOutSubtaskCommand(taskid), 30000, TimeUnit.SECONDS);
//
//            if(returnStatus.code == Status.ERROR) {
//                System.err.println(returnStatus.msg);
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Scaling out response is received!");
    }

    public boolean sendScalingInSubtaskCommand(int taskid) throws TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
        System.out.println("Scaling in message has been sent!");
        try {
            Status returnStatus = (Status)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ScalingInSubtaskCommand(taskid), 30000, TimeUnit.SECONDS);
            System.out.println("Scaling in response is received!");
            return returnStatus.code == Status.OK;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean scalingInSubtask(int taskid) throws TException {
        if(!_elasticTaskIdToWorkerLogicalName.containsKey(taskid)) {
            throw new TaskNotExistException("Task " + taskid + " does not exist!");
        }
        System.out.println("Scaling in message has been sent!");
        try {
//            Status returnStatus = (Status)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new ScalingInSubtaskCommand(taskid), 30000, TimeUnit.SECONDS);
//            System.out.println("Scaling in response is received!");
//        return returnStatus.code == Status.OK;
            handleExecutorScalingInRequest(taskid);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
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

    String extractIpFromWorkerLogicalName(String workerName) {
        for(String ip: _ipToWorkerLogicalName.keySet()) {
            if(_ipToWorkerLogicalName.get(ip).contains(workerName))
                return ip;
        }
        return null;
    }

    public Histograms getDistributionHistogram(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        try {
            return (Histograms)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new DistributionQueryCommand(taskid));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public RoutingTable getRoutingTable(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        try {
            Object received = sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new RoutingTableQueryCommand(taskid));
            if(!(received instanceof RoutingTable)) {
                System.err.println(String.format("Expected: RoutingTable, get: %s , context: %s", received.getClass().toString(), received.toString()));
                return null;
            }
            return (RoutingTable)received;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public RoutingTable getOriginalRoutingTable(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        RoutingTableQueryCommand command = new RoutingTableQueryCommand(taskid);
        command.completeRouting = false;
        try {
            return (RoutingTable)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), command, 3000, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Histograms getBucketDistribution(int taskid) throws TaskNotExistException{
        if(!_taskidToActorName.containsKey(taskid))
            throw new TaskNotExistException("task " + taskid + " does not exist!");
        try {
              return (Histograms)sendAndReceive(getContext().actorFor(_nameToPath.get(_taskidToActorName.get(taskid))), new BucketDistributionQueryCommand(taskid));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getRouteHosterName(int taskid, int route) {
        return _taskidRouteToWorker.get(taskid + "." + route);
    }

    private void printIpToWorkerLogicalName() {
        System.out.println("_ipToWorkerLogicalName: ");
        for(String ip: _ipToWorkerLogicalName.keySet()) {
            System.out.println(String.format("%s: %s", ip, _ipToWorkerLogicalName.get(ip)));
        }
    }

}
