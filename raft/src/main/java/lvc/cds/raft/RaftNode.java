package lvc.cds.raft;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import javax.lang.model.util.ElementScanner6;

import com.github.cliftonlabs.json_simple.JsonException;
import com.github.cliftonlabs.json_simple.JsonObject;
// import com.google.protobuf.Message;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lvc.cds.KVS;
import lvc.cds.raft.proto.AppendEntriesMessage;
import lvc.cds.raft.proto.ClientMessage;
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
    private KVS kvs;
    private String me;



    protected int port;

    protected RaftNode(int port, String me, List<String> peers) throws IOException {
        // incoming RPC messages come to this port
        this.port = port;
        this.messages = new ConcurrentLinkedQueue<>();
        this.state = NODE_STATE.FOLLOWER;
        this.leaderId = me;
        this.me = me;
        
        try{
            this.kvs = new KVS("kvsstorage");
        }catch(JsonException e){

        }
        
        

        // a map containing stubs for communicating with each of our peers
        this.peers = new HashMap<>();
        for (String p : peers) {
            if (!p.equals(me))
                this.peers.put(p, new PeerStub(p, port, messages));
        }

        this.peersConnected = false;
        if (!peersConnected)
        {
            connectRpcChannels();
            peersConnected = true;
        }

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
        rrpc = new RaftRPC(messages, this);
        
    }

    public void run() throws IOException {
        // start listening for incoming messages now, so that others can connect
        startRpcListener();

        // note: we defer making any outgoing connections until we need to send
        // a message. This should help with startup.

        // a state machine implementation.
        state = NODE_STATE.FOLLOWER;

        while (state != NODE_STATE.SHUTDOWN) {
            System.out.println(state);
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
        for (PeerStub peer : peers.values()) {
            peer.connect();
        }
        peersConnected = true;
    }

    private void shutdownRpcStubs() {
        try {
            for (PeerStub peer : peers.values()) {
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

            // If we haven't connected yet...
            if (!peersConnected)
            {
                connectRpcChannels();
                peersConnected = true;
            }

            heartbeat = 25000 + rand.nextInt(1500);
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
            int lastNewEntry = log.size()-1;
            Message m = messages.poll();
            if (m != null) {
                if (m.getMsg().equals(""))
                    break;
                if(m.getType().equals("appendEntries"))
                {
                    MessageAppendEntries ae = (MessageAppendEntries) m;
                    //if success
                    if(term < ae.getTerm())
                    {
                        term = ae.getTerm();
                        persistentState(term, votedFor);
                    }
                    ArrayList<Command> toAdd = ae.getAllEntries();
                    for(Command c: toAdd)
                    {
                        log.add(c);
                        persistentLog(log.size()-1);
                        lastNewEntry = log.size()-1;
                    }
                    if(ae.getLeaderCommitIndex() > commitIndex)
                    {
                        if(ae.getLeaderCommitIndex() > lastNewEntry)
                            commitIndex = lastNewEntry;
                        else
                            commitIndex = ae.getLeaderCommitIndex();
                    }
                    start = System.currentTimeMillis();
                    
                }
                else if(m.getType().equals("requestVote"))
                {
                    MessageRequestVote rv = (MessageRequestVote) m;
                    //verify that response is handled an dwe are granting vote
                    if(rv.getTerm() > term)
                    {
                        term = rv.getTerm();
                    }
                    votedFor = rv.getCandidateId();
                    persistentState(term, votedFor);
                    start = System.currentTimeMillis();
                }

                //message from client


            }


            //can we apply any new logs?
            if(commitIndex > lastApplied)
            {
                parseToKVS(log.get(lastApplied+1));
                lastApplied++;
            }


        }
        return NODE_STATE.CANDIDATE;
    }

    // lo
    private NODE_STATE leader() {
        // any setup that is needed before we start out event loop as we just
        // became leader. For instance, initialize the nextIndex and matchIndex
        // hashmaps.
        //term++;
        Map<String, Long> HeartBeat = new HashMap<>(peers.size());
        Map<String, Integer> nextIndex = new HashMap<>(peers.size());
        Map<String, Integer> matchIndex = new HashMap<>(peers.size());
        for(String peer : peers.keySet()){
            HeartBeat.put(peer, System.currentTimeMillis());
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
        for (String peer : peers.keySet()) {
            peers.get(peer).sendAppendEntries(term, leaderId, nextIndex.get(peer) - 1, log.get(nextIndex.get(peer) - 1).getTerm(), new ArrayList<String>()//empty entries for heartbeat
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
                parseToKVS(log.get(lastApplied + 1));
                lastApplied++;

                //need to respond to client here
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
                if (m.getMsg().equals(""))
                    break;
                if (m.getType().equals("client")){
                    MessageClient cM = (MessageClient) messages.poll();
                    // split(" , 2") should give list with [0] = method and [1] = body
                    String[] splited = cM.getClientRequest().split(" ", 2);
                    log.add(new Command(term, log.size(), splited[0], splited[1]));
                    persistentLog(log.size()-1);
                    // No confirmation to client needed
                    System.out.println("Client message: " + term + ", " + splited[0] + ", " + splited[1]);
                }
                if (m.getType().equals("appendEntriesResponse")){
                    AppendEntriesResponse aerM = (AppendEntriesResponse) messages.poll();
                    if (aerM.getSuccess()){ // incrementing mindex, nindex, if success
                        matchIndex.replace(aerM.getPeer(), aerM.getPrevLogIdx() + aerM.getSize());
                        nextIndex.replace(aerM.getPeer(), nextIndex.get(aerM.getPeer()) + aerM.getSize());
                    }
                    else if (!aerM.getSuccess()){ // decrement if success is false
                        if(term < aerM.getTerm())
                        {
                            term = aerM.getTerm();
                            persistentState(term, votedFor);
                            return NODE_STATE.FOLLOWER;
                        }
                        nextIndex.replace(aerM.getPeer(), nextIndex.get(aerM.getPeer()) - 1);
                    }
                }
                if (m.getType().equals("appendEntries") ){
                    MessageAppendEntries aeM = (MessageAppendEntries) m;
                    if (aeM.getTerm() > term){ // we dont do messages.poll() so message can stay on queue
                        state = NODE_STATE.FOLLOWER;
                        break;
                    }
                    //TODO Does anything happen when terms or equal or less then?
                } else if(m.getType().equals("requestVote")){
                    MessageRequestVote rvM = (MessageRequestVote) m;
                    if (rvM.getTerm() > term){ 
                        term = rvM.getTerm();
                        persistentState(term,votedFor);
                        state = NODE_STATE.FOLLOWER;
                        break;
                    }
                }
                else{
                    messages.poll();
                }
            } 
            // step 3: see if we need to send any messages out. Iterate through
            //         nextIndex and send an appendEntries message to anyone who 
            //         seems out of date. If our heartbeat timeout has expired, send
            //         a message to everyone. if we do send an appendEntries, 
            //         update the heartbeat timer.
            for(String peer : nextIndex.keySet()){
                if(System.currentTimeMillis() > HeartBeat.get(peer) + 10000) {
                    if (nextIndex.get(peer) <= log.size() - 1){ // if the next to send to peer value is <= latest entry to log(log.size() - 1)
                        ArrayList<String> logEntriesToAdd = new ArrayList<>();
                        for(int i = nextIndex.get(peer); i <= log.size() - 1; i++){ // assuming log first index is 1
                            logEntriesToAdd.add(String.valueOf(log.get(i).getTerm()));
                            logEntriesToAdd.add(String.valueOf(log.get(i).getIndex()));
                            logEntriesToAdd.add(log.get(i).getMethod());
                            logEntriesToAdd.add(log.get(i).getBody());
                        }
                        peers.get(peer).sendAppendEntries(term, leaderId, nextIndex.get(peer) - 1, log.get(nextIndex.get(peer) - 1).getTerm(),
                        logEntriesToAdd, commitIndex);
                        HeartBeat.replace(peer, System.currentTimeMillis());
                        //start = System.currentTimeMillis(); //TODO why update heartbeat timer if only one node is being contacted
                        
                    }
                }

            }
            //Heartbeat Timeout
            for(String peer : HeartBeat.keySet()){
                if(System.currentTimeMillis() > HeartBeat.get(peer) + 10000)
                {
                    peers.get(peer).sendAppendEntries(term, leaderId, nextIndex.get(peer) - 1, log.get(nextIndex.get(peer) - 1).getTerm(), new ArrayList<String>()//empty entries for heartbeat
                    , commitIndex);
                    HeartBeat.replace(peer, System.currentTimeMillis()); // reseting heartbeat timer
                }
            }
            // step 4: iterate through matchIndex to see if a majority of entries
            //         are > commitIndex (with term that is current). If so, 
            //         ++commitIndex. There are more aggressive ways to do this,
            //         but this will do.
            int counter = 1; // 1 because of counting self as part of majority
            for (int mIndex : matchIndex.values())
            {
                if (mIndex > commitIndex){counter++;}
            }
            if (log.size() -1 > commitIndex && counter >= ((peers.size() + 1)/2)+1){ commitIndex++;}
        }

        return NODE_STATE.FOLLOWER;
        
    }

    private NODE_STATE candidate() {
        // an event loop that processes incoming messages and timeout events
        // according to the raft rules for leaders.

        term++;
        votedFor = me;
        persistentState(term, votedFor);
        Random rand = new Random();
        long heartbeat;
        long start = System.currentTimeMillis();

        int lastLogIndex;
        int lastLogTerm;
        int votes = 1;
        if(log.size() > 0)
        {
            lastLogIndex = log.size()-1;
            lastLogTerm = log.get(log.size()-1).getTerm();
        }
        else{
            lastLogIndex = 0;
            lastLogTerm = 0;
        }


        if(votes > (peers.size() +1 / 2.0))
            return NODE_STATE.LEADER;
            
        for (PeerStub peer : peers.values()) {
            peer.sendRequestVote(term, me, lastLogIndex, lastLogTerm);
        }

        while(true)
        {
            // If we haven't connected yet...
            if (!peersConnected)
            {
                connectRpcChannels();
                peersConnected = true;
            }

            heartbeat = 25000 + rand.nextInt(1500);
            if(System.currentTimeMillis() - start > heartbeat)
            {
                return NODE_STATE.CANDIDATE;
            }

            Message m = messages.peek(); // so message is left on queue if needed
            if (m != null) {
                if (m.getMsg().equals(""))
                    break;
                if(m.getType().equals("requestVoteResponse"))
                {
                    RequestVoteResponse vr = (RequestVoteResponse) messages.poll();
                    if(vr.getTerm() > term)
                    {
                        term = vr.getTerm();
                        persistentState(term, votedFor);
                        return NODE_STATE.FOLLOWER;
                    }
                    if(vr.getSuccess())
                        votes++;
                    if(votes > (peers.size() +1) / 2.0)
                        return NODE_STATE.LEADER;
                }
                else if(m.getType().equals("appendEntries"))
                {
                    return NODE_STATE.FOLLOWER;
                }
                else if(m.getType().equals("requestVote"))
                {
                    return NODE_STATE.FOLLOWER;
                }
                else{
                    messages.poll();
                }

            }

        }

        return NODE_STATE.FOLLOWER;
    }


    public void persistentLog(int index)
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
            myWriter.write("\n");
            myWriter.close();
          } catch (IOException e) {

          }
    }

    public void writeLogs()
    {
        try {
            FileWriter myWriter = new FileWriter("persistentLog.txt");
            for(Command c: log)
            {
                myWriter.write("" + c.getTerm());
                myWriter.write("\n");
                myWriter.write("" + c.getIndex());
                myWriter.write("\n");
                myWriter.write("" + c.getMethod());
                myWriter.write("\n");
                myWriter.write("" + c.getBody());
                myWriter.write("\n");
            }
            myWriter.close();
          } catch (IOException e) {

          }
    }

    public void persistentState(int currentTerm, String votedFor)
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
            Command emptyInitalLog = new Command(0, 0, "index0", "index0");
            log.add(emptyInitalLog);
            persistentLog(0);
            myWriter.close();
          } catch (IOException e) {

          }
    }

    public Command getPrevLog(int n)
    {      
        return log.get(log.size()-1-n); 
    }

    public void deleteLog(int n)
    {
        for(int i = n; i < log.size(); i++)
            log.remove(i);
        writeLogs();
    }

    public int getLogSize()
    {
        return log.size();
    }

    public Command getLog(int n)
    {
        return log.get(n);
    }

    public int getTerm()
    {
        return term;
    }

    public int getPrevLogTerm()
    {
        return log.get(log.size()-1).getTerm();
    }

    public boolean prevLogExist(int prevLogIndex, int prevLogTerm){
        if (prevLogIndex > log.size()-1){ // greater as index starts at 1
            return false;
        }
        if (log.get(prevLogIndex).getTerm() != prevLogTerm){
            return false;
        }
        return true;
    }

    public int getCommit()
    {
        return commitIndex;
    }

    public String getState()
    {
        switch (state) {
            case FOLLOWER:
                return "follower";
            case LEADER:
                return "leader";
            case CANDIDATE:
                return "candidate";
            case SHUTDOWN:
                return "";
            }
        return "";
    }

    public void parseToKVS(Command command){
        // [0] should be KEY and [1] should be key and [2] value if add
        // [0] = KEY if remove
        // [0] = KEY, [1] = field, [2] = value if modify request
        String[] splited = command.getBody().split(" ");
        
        if(command.getMethod().equals("ADD")){
            JsonObject hold = new JsonObject();
            hold.put(splited[1], splited[2]);
            kvs.add(splited[0], hold);
        }else if(command.getMethod().equals("DEL")){
            kvs.remove(splited[0]);
        } else if(command.getMethod().equals("clear")){
            kvs.clear();
        } else if (command.getMethod().equals("modify")){
            kvs.modify(splited[0], splited[1], splited[2]);
        }
        try {
            kvs.writeToDisk("kvsstorage");
          } catch (IOException e) {

          }
        
    }
}
