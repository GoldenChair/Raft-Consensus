package lvc.cds.raft;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

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


    protected int port;

    protected RaftNode(int port, String me, List<String> peers) throws IOException {
        // incoming RPC messages come to this port
        this.port = port;
        this.messages = new ConcurrentLinkedQueue<>();

        // a map containing stubs for communicating with each of our peers
        this.peers = new HashMap<>();
        for (var p : peers) {
            if (!p.equals(me))
                this.peers.put(p, new PeerStub(p, port, messages));
        }
        // lazily connect to peers.
        this.peersConnected = false;
        log = new ArrayList<>();
        commitIndex = 0;
        lastApplied = 0;
        
        this.state = NODE_STATE.FOLLOWER;
        
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
                .addService(new RaftRPC(messages))
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
        long heartbeat = 10000 + rand.nextInt(50);
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
                if (m.msg.equals(""))
                    break;

                System.out.println("handled message");
                System.out.println(m.msg);
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

    private NODE_STATE leader() {
        // any setup that is needed before we start out event loop as we just
        // became leader. For instance, initialize the nextIndex and matchIndex
        // hashmaps.
        int[] nextIndex = new int[peers.size()];
        int[] matchIndex = new int[peers.size()];
        for(int i: nextIndex)
            i = log.size();
        for(int i: matchIndex)
            i = 0;

        long heartbeat = 5000;
        long newMessages = 1000;
        long[] time = new long[peers.size()];
        for(long t: time)
            t = System.currentTimeMillis();


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
            // logs. As we commit logs, Send message to client that the job is done 
            // for that log entry.Increase lastApplied
            //
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
            // 
            // step 3: see if we need to send any messages out. Iterate through
            //         nextIndex and send an appendEntries message to anyone who 
            //         seems out of date. If our heartbeat timeout has expired, send
            //         a message to everyone. if we do send an appendEntries, 
            //         update the heartbeat timer.
            //
            // step 4: iterate through matchIndex to see if a majority of entries
            //         are > commitIndex (with term that is current). If so, 
            //         ++commitIndex. There are more aggressive ways to do this,
            //         but this will do.
        }
    }

    private NODE_STATE candidate() {
        // an event loop that processes incoming messages and timeout events
        // according to the raft rules for leaders.
        return NODE_STATE.LEADER;
    }


    public boolean committedLog(int index)
    {
        //write log to file
    }

    public boolean commitState(int currentTerm, String votedFor)
    {
        //write to file
    }

    
}
