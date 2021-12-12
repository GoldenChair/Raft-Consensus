package lvc.cds.raft;

import lvc.cds.raft.proto.*;
import lvc.cds.raft.proto.AppendEntriesMessage;

import java.util.concurrent.ConcurrentLinkedQueue;

import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCImplBase;

public class RaftRPC extends RaftRPCImplBase {
    ConcurrentLinkedQueue<Message> messages;
    private int term;
    private int prevLogIdx;
    private int prevLogTerm;


    public RaftRPC(ConcurrentLinkedQueue<Message> messages, int term) {
        this.messages = messages;
        this.term = term;
    } 

    @Override
    public void appendEntries(AppendEntriesMessage req, StreamObserver<Response> responseObserver) {
        String msg = "" + req.getTerm() + " " + req.getLeaderID() + " " + req.getPrevLogIdx() + " " + req.getPrevLogTerm() + " " + req.getAllEntries() + "  " + req.getLeaderCommitIdx();


        messages.add(new AppendEntriesMessage(msg));
        
        Response reply = Response.newBuilder().setSuccess(false).setTerm(-1).build();
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
