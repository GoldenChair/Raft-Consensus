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
        
        String success = false;
        

        int pli = req.getPrevLogIdx();
        int plt = req.getPrevLogTerm();
        Command c;
        if(term <= req.getTerm())
        {
            int size = node.getLogSize();
            for(int i = 0; i < size; i++)
            {
                c = node.getPrevLog(i);
                if(pli == c.getIndex() && plt == c.getTerm())
                {
                    success = true;
                    node.deleteExtraLogs(pli);
                    break;
                }
            }
            
        }

        if(success)
        {
            ArrayList<String> entries = req.getAllEntries();
            String msg = "" + req.getTerm() + " " + req.getLeaderID() + " " + pli + " " + plt;
            for(String s: entries)
            {
                msg += ":" + s;
            }
            
            msg += "  " + req.getLeaderCommitIdx();
            messages.add(new MessageAppendEntries(msg));
        }
            

        Response reply = Response.newBuilder().setSuccess(success).setTerm(term).build();
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

    public void setTerm(int t)
    {
        term = t;
    }
    

    
}
