package lvc.cds.raft;

import lvc.cds.raft.proto.*;

import java.util.concurrent.ConcurrentLinkedQueue;

import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCImplBase;

public class RaftRPC extends RaftRPCImplBase {
    ConcurrentLinkedQueue<Message> messages;


    public RaftRPC(ConcurrentLinkedQueue<Message> messages) {
        this.messages = messages;
    }

    @Override
    public void appendEntries(AppendEntriesMessage req, StreamObserver<Response> responseObserver) {
        String msg = "" + req.getTerm() + " " + req.getLeaderCommitIdx() + " "
                    + req.getPrevLogIdx() + " " + req.getPrevLogTerm() + "\n"
                    + req.getLeaderID() + "\n"
                    + req.getEntries(0) + " "
                + req.getEntries(1);

        messages.add(new Message(msg));
        
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
    

    
}
