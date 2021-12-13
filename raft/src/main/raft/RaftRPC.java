package lvc.cds.raft;

import lvc.cds.raft.proto.*;
import lvc.cds.raft.proto.AppendEntriesMessage;

import java.util.concurrent.ConcurrentLinkedQueue;

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
        
        String success = true;
        boolean addMessage = true;

        String msg = "" + req.getTerm() + " " + req.getLeaderID() + " " + req.getPrevLogIdx() + " " + req.getPrevLogTerm() + " " + req.getAllEntries() + "  " + req.getLeaderCommitIdx();

        if(term > req.getTerm())
        {
            success = false;
            addMessage = false;
        }

        //find prevLog or find that we need to delete

        
        if(addMessage)
            messages.add(new AppendEntriesMessage(msg));
        
        Response reply = Response.newBuilder().setSuccess(false).setTerm(term).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
    
    @Override
    public void requestVote(RequestVoteMessage req, StreamObserver<Response> responseObserver) {
        
        
        Response reply = Response.newBuilder().setSuccess(false).setTerm(-1).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    public void setTerm(int t)
    {
        term = t;
    }
    

    
}
