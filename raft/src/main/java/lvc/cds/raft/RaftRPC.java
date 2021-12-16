package lvc.cds.raft;

import lvc.cds.raft.proto.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;

import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCImplBase;

public class RaftRPC extends RaftRPCImplBase {
    ConcurrentLinkedQueue<Message> messages;
    private int term;
    private RaftNode node;

    public RaftRPC(ConcurrentLinkedQueue<Message> messages, int term, RaftNode node) {
        this.messages = messages;
        this.term = term;
        this.node = node;
    } 

    @Override
    public void appendEntries(AppendEntriesMessage req, StreamObserver<Response> responseObserver) {
        boolean success = true;
        term = req.getTerm();
        int prevLogIdx = req.getPrevLogIdx();
        int prevLogTerm = req.getPrevLogTerm();

        if (term < node.getTerm() || !node.prevLogExist(prevLogIdx, prevLogTerm)){ // Recevier implementation 1. & 2. in raft paper pg.4
            success = false;
        }

        if (success){
            String leaderId = req.getLeaderID();
            int leaderCommitIndex = req.getLeaderCommitIdx();
            ArrayList<Command> commands = new ArrayList<>();
            
            String msg = "" + req.getTerm() + " " + req.getLeaderID() + " "
            + req.getPrevLogIdx() + " " + req.getPrevLogTerm() + "\n"
            + req.getLeaderCommitIdx() + "\n" + req.getEntriesList();

            ProtocolStringList commandFields = req.getEntriesList();

            // May be better way to make command using the Entries list of strings
            for(int i = 0; i < req.getEntriesCount(); i += 4 ){ // should always be multiple of 4 as Command is split into 4 pieces
                commands.add(new Command(commandFields.get(i), commandFields.get(i+1), commandFields.get(i+2), commandFields.get(i+3)));
            }

            messages.add(new MessageAppendEntries(msg,term, leaderId, prevLogIdx, prevLogTerm, leaderCommitIndex, commands));
        }

        Response reply = Response.newBuilder().setSuccess(success).setTerm(node.getTerm()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
    
    @Override
    public void requestVote(RequestVoteMessage req, StreamObserver<Response> responseObserver) {
        String success = false;

        int lli = req.getLastLogIndex();
        int llt = req.getLastLogTerm();
        if(term <= req.getTerm())
        {
            Command c = node.getPrevLog(0);
            if(c.getIndex() <= lli && c.getTerm() <= llt)
            {
                success = true;
            }
        }

        if(success)
        {
            String msg = req.getTerm() + " " + req.getCandidateID() + " " + lli + " " + llt;
            messages.add(new MessageRequestVote(msg));
        }
        Response reply = Response.newBuilder().setSuccess(success).setTerm(term).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    // This should recive a message from client and it to messages
    // should be able to recieve it as clientMessage and message.poll().log
    @Override
    public void clientMessage(ClientMessage req, StreamObserver<Response> responseObserver){
        messages.add(new MessageClient(req.getLog()));
    }


    public void setTerm(int t)
    {
        term = t;
    }
    

    
}
