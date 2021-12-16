package lvc.cds.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Message;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.AppendEntriesMessage;
import lvc.cds.raft.proto.RaftRPCGrpc;
import lvc.cds.raft.proto.Response;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCStub;
import java.util.Random;
import java.util.Scanner;

public class RaftNode {
    public enum NODE_STATE {
        FOLLOWER, LEADER, CANDIDATE, SHUTDOWN
    }

    private ConcurrentLinkedQueue<Message> messages;
    private ArrayList<Command> log;

    private NODE_STATE state;
    private Server rpcServer;
    private Map<String, PeerStub> peers;
    private boolean peersConnected;
    private int commitIndex;
    private int lastApplied;
    private RaftRPC rrpc;
    private int term;
    private String votedFor;
    private String leaderId;

    private File persistentState;
    private File logStorage;



    protected int port;

    protected RaftNode(int port, String me, List<String> peers) throws IOException {
        // incoming RPC messages come to this port
        this.port = port;
        this.messages = new ConcurrentLinkedQueue<>();
        this.state = NODE_STATE.FOLLOWER;
        this.leaderId = me;

        // a map containing stubs for communicating with each of our peers
        this.peers = new HashMap<>();
        for (var p : peers) {
            if (!p.equals(me))
                this.peers.put(p, new PeerStub(p, port, messages));
        }

        this.peersConnected = false;

        //volatile
        commitIndex = 0;
        lastApplied = 0;

        // initial persistent state (currentTerm and votedFor) for node
        try{
            persistentState = new File("persistentState.txt");
            Scanner sc = new Scanner(persistentState);
            term = sc.nextInt();
            sc.nextLine();
            votedFor = sc.nextLine();

        }
        catch(IOException e)
        {
            term = 0;
            votedFor = "none";
            createPersistentState();
        }

        log = new ArrayList<>();
        int t;
        int i;
        String method;
        String body;
        File logStorage; // Possible to remove?
        // initial persistenLog for server
        // when starting server all logs from pLog is added to memory.
        try{
            logStorage = new File("persistentLog.txt");
            Scanner sc = new Scanner(logStorage);
            while(sc.hasNextLine())
            {
                t = sc.nextInt();
                i = sc.nextInt();
                sc.nextLine();
                method = sc.nextLine();
                body = sc.nextLine();
                log.add(new Command(t, i, method, body));
            }

        }
        catch(IOException e)
        {
            createEmptyLog();
        }
        
        //what else do we need to send
        // do we need to send term when we have this?
        rrpc = new RaftRPC(messages, term, this);
        
        
    }

    public void run() throws IOException {
        // start listening for incoming messages now, so that others can connect
        startRpcListener();

        // note: we defer making any outgoing connections until we need to send
        // a message. This should help with startup.

        // a state machine implementation.
        state = NODE_STATE.FOLLOWER;

        while (state != NODE_STATE.SHUTDOWN) {
            switch (state) {
            case FOLLOWER:
                state = follower();
                break;
            case LEADER:
                state = leader();
                break;
            case CANDIDATE:
                state = candidate();
                break;
            case SHUTDOWN:
                break;
            }
        }

        // shut down the server
        try {
            shutdownRpcListener();
            shutdownRpcStubs();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void connectRpcChannels() {
        for (var peer : peers.values()) {
            peer.connect();
        }
        peersConnected = true;
    }

    private void shutdownRpcStubs() {
        try {
            for (var peer : peers.values()) {
                peer.shutdown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startRpcListener() throws IOException {
        // build and start the server to listen to incoming RPC calls.
        rpcServer = ServerBuilder.forPort(port)
                .addService(rrpc)
                .build()
                .start();

        // add this hook to be run at shutdown, to make sure our server
        // is killed and the socket closed.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdownRpcListener();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    private void shutdownRpcListener() throws InterruptedException {
        if (rpcServer != null) {
            rpcServer.shutdownNow().awaitTermination();
        }
    }

    private NODE_STATE follower() {
        Random rand = new Random();
        long heartbeat;
        long start = System.currentTimeMillis();
        // an event loop that processes incoming messages and timeout events
        // according to the raft rules for followers.

        while (true) {
            heartbeat = 10000 + rand.nextInt(50);
            if(System.currentTimeMillis() - start > heartbeat)
            {
                return NODE_STATE.CANDIDATE;
            }
            /*try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }*/

            // first, see if there are incoming messages or replies
            // to process
            Message m = messages.poll();
            if (m != null) {
                if (m.getMsg().equals(""))
                    break;
                if(m.getType().equals("appendEntries"))
                {
                    //if success
                    int lastNewEntry;
                    if(term < m.getTerm())
                    {
                        term = m.getTerm();
                        persistentState(term, votedFor);
                        rrpc.setTerm(term);
                    }
                    ArrayList<Command> toAdd = m.getEntries();
                    for(Command c: toAdd)
                    {
                        log.add(c);
                        persistentLog(log.size()-1);
                        lastNewEntry = log.size()-1;
                    }
                    if(m.getLeaderCommit() > commitIndex)
                    {
                        if(m.getLeaderCommit() > lastNewEntry)
                            commitIndex = lastNewEntry;
                        else
                            commitIndex = m.getLeaderCommit();
                    }
                    
                    
                }
                else if(m.getType().equals("requestVote"))
                {
                    //verify that response is handled an dwe are granting vote
                    if(m.getTerm() > term)
                    {
                        term = m.getTerm();
                        rrpc.setTerm(term);
                    }
                    votedFor = m.getCandidate();
                    persistentState(term, votedFor);
                }


            }

            //can we commit any new logs?
            //if(commitIndex < log.size())

            //can we apply any new logs?
            //if(commitIndex > lastApplied)

            // If we haven't connected yet...
            /*if (!peersConnected)
                connectRpcChannels();*/


        }
        return NODE_STATE.CANDIDATE;
    }

    // lo
    private NODE_STATE leader() {
        long start = System.currentTimeMillis();
        // any setup that is needed before we start out event loop as we just
        // became leader. For instance, initialize the nextIndex and matchIndex
        // hashmaps.
        term++;
        Map<String, Integer> nextIndex = new HashMap<>(peers.size());
        Map<String, Integer> matchIndex = new HashMap<>(peers.size());
        for(String peer : peers.keySet()){
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, 0);
        }
        // If we haven't connected yet...
        if (!peersConnected)
            connectRpcChannels();

        // upon election; send initial empty AppendEntries RPC (heartbeat)
        // Maybe create zero parameter constructer appendEntries rpc for heartbeat
        // send a message to every peer for initial heartbeat
        // heartbeat is appendEntries with no log entries
        for (var peer : peers.values()) {
            peers.get(peer).sendAppendEntries(term, leaderId, nextIndex.get(peer) - 1, log[nextIndex.get(peer) - 1].term, new ArrayList<String>()//empty entries for heartbeat
                    , commitIndex); 
        }


        // notes: We have decisions to make regarding things like: how many queues do
        // we want (one is workable, but several (e.g., one for client messages, one 
        // for incoming raft messages, one for replies) may be easier to reason about).
        // We also need to determine what type of thing goes in a queue -- we could
        // for instance use a small inheritance hierarchy (a base class QueueEntry with 
        // derived classes for each type of thing that can be enqueued), or make enough
        // queues so that each is homogeneous. Or, a single type of queue entry with 
        // enough fields in it that it could hold any type of message, and facilities in 
        // place to determine what we're looking at.

        while (true) {
            // step one: check out commitIndex to see if we can commit any
            // logs. As we commit logs, Increase lastApplied
            while (commitIndex > lastApplied){
                //TODO apply log[lastApplied + 1] to KVS
                // lastApplied++
            }
            // step 2: check to see if any messages or replies are present
            // if so:
            //    - if it is a request from a client, then add to our log
            //    - if it is a response to an appendEntries, process it
            //      by first checking the term, then (if success is true)
            //      incrementing nextIndex and matchIndex for that node or
            //      (if success is false) decrementing nextIndex for the follower.
            //    - if it is anything else (e.g., a requestVote or appendEntries)  
            //      then we want to check the term, and if appropriate, convert to
            //      follower (leaving this message on the queue!)
            Message m = messages.peek(); // so message is left on queue if needed
            if (m != null) {
                if (m.msg.equals(""))
                    break;
                if (m.getType().equals("client")){
                    m = messages.poll();
                    // m.log should be the string recieved from client
                    log.add(new Command(term, log.size(), m.clientRequest, "")); //TODO not sure what body is supposed to be
                    persistentLog(log.size());
                    // No confirmation to client needed
                }
                if (m.getType().equals("appendEntriesResponse")){
                    m = messages.poll();
                    if (m.getSuccess()){ // incrementing mindex, nindex, if success
                        matchIndex.replace(m.getPeer(), matchIndex.get(m.getPeer()) + 1);
                        nextIndex.replace(m.getPeer(), nextIndex.get(m.getPeer()) + 1);
                    }
                    else if (!m.getSuccess()){ // decrement if success is false
                        nextIndex.replace(m.getPeer(), nextIndex.get(m.getPeer()) - 1);
                    }
                    else{ //TODO is there anything other case to account for?

                    }
                }
                if (m.getType().equals("appendEntries") || m.getType().equals("requestVote")){
                    if (m.getTerm() > term){ // we dont do messages.poll() so message can stay on queue
                        state = NODE_STATE.FOLLOWER;
                        break;
                    }
                    //TODO Does anything happen when terms or equal or less then?
                }

                System.out.println("handled message");
                System.out.println(m.msg);
            } 
            // step 3: see if we need to send any messages out. Iterate through
            //         nextIndex and send an appendEntries message to anyone who 
            //         seems out of date. If our heartbeat timeout has expired, send
            //         a message to everyone. if we do send an appendEntries, 
            //         update the heartbeat timer.
            for(String peer : nextIndex.keySet()){
                if (nextIndex.get(peer) <= log.size() - 1){ // if the next to send to peer value is <= latest entry to log(log.size() - 1)
                    ArrayList<String> logEntriesToAdd = new ArrayList<>();
                    for(int i = nextIndex.get(peer); i <= log.size(); i++){ // assuming log first index is 1
                        logEntriesToAdd.add(log[i].getTerm());
                        logEntriesToAdd.add(log[i].getIndex());
                        logEntriesToAdd.add(log[i].getMethod());
                        logEntriesToAdd.add(log[i].getBody());
                    }
                    peers.get(peer).sendAppendEntries(term, leaderId, nextIndex.get(peer) - 1, log[nextIndex.get(peer) - 1].term, logEntriesToAdd, commitIndex);
                    //start = System.currentTimeMillis(); //TODO why update heartbeat timer if only one node is being contacted
                    
                }

            }
            //Heartbeat Timeout
            if(System.currentTimeMillis() + 5000 > start)
            {
                for(String peer : nextInde.keySet()){ //send heartbeat out to every peer 

                    peers.get(peer).sendAppendEntries(term, leaderId, nextIndex.get(peer) - 1, log[nextIndex.get(peer) - 1].term, new ArrayList<String>()//empty entries for heartbeat
                    , commitIndex);
                }
                start = System.currentTimeMillis();// reseting heartbeat timer
            }
            // step 4: iterate through matchIndex to see if a majority of entries
            //         are > commitIndex (with term that is current). If so, 
            //         ++commitIndex. There are more aggressive ways to do this,
            //         but this will do.
            int counter = 1; // 1 because of counting self as part of majority
            for (int mIndex : matchIndex.values()){
                if (mIndex > commitIndex){counter++;}
            }
            if (counter >= ((peers + 1)/2)+1){ commitIndex++;}
        }
    }

    private NODE_STATE candidate() {
        // an event loop that processes incoming messages and timeout events
        // according to the raft rules for leaders.
        return NODE_STATE.LEADER;
    }


    public boolean persistentLog(int index)
    {
        Command c = log.get(index);
        try {
            FileWriter myWriter = new FileWriter("persistentLog.txt", true);
            myWriter.write("" + c.getTerm());
            myWriter.write("\n");
            myWriter.write("" + c.getIndex());
            myWriter.write("\n");
            myWriter.write("" + c.getMethod());
            myWriter.write("\n");
            myWriter.write("" + c.getBody());
            myWriter.close();
          } catch (IOException e) {

          }
    }

    public boolean persistentState(int currentTerm, String votedFor)
    {
        try {
            FileWriter myWriter = new FileWriter("persistentState.txt");
            myWriter.write("" + currentTerm);
            myWriter.write("\n");
            myWriter.write(votedFor);
            myWriter.close();
          } catch (IOException e) {

          }
    }

    public void createPersistentState()
    {
        try {
            FileWriter myWriter = new FileWriter("persistentState.txt");
            myWriter.write("0");
            myWriter.write("\n");
            myWriter.write("none");
            myWriter.close();
          } catch (IOException e) {

          }
    }

    public void createEmptyLog()
    {
        try {
            // If there was no persistentLog on node create one and put inital empty Command in
            FileWriter myWriter = new FileWriter("persistentLog.txt");
            Command emptyInitalLog = new Command(0, 0, "", "");
            log.add(emptyInitalLog);
            persistentLog(0);
            myWriter.close();
          } catch (IOException e) {

          }
    }

    public Command getPrevLog(int n)
    {      
        return log.get(log.size-1-n); 
    }

    public void deleteExtraLogs(int n)
    {
        for(int i = log.size() - 1; i > n; i--)
        {
            log.remove(i);
        }
    }

    public int getLogSize()
    {
        return log.size();
    }

    public int getTerm()
    {
        return term;
    }

    public boolean prevLogExist(int prevLogIndex, int prevLogTerm){
        if (prevLogIndex > log.size()){ // greater as index starts at 1
            return false;
        }
        if (log.get(prevLogIndex).getTerm() != prevLogTerm){
            return false;
        }
        return true;
    }

    
}
