package lvc.cds.raft;

import lvc.cds.raft.proto.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.protobuf.Empty;
import com.google.protobuf.ProtocolStringList;

import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCImplBase;

public class RaftRPC extends RaftRPCImplBase {
    ConcurrentLinkedQueue<Message> messages;
    private RaftNode node;

    public RaftRPC(ConcurrentLinkedQueue<Message> messages, RaftNode node) {
        this.messages = messages;
        this.node = node;
    } 

    @Override
    public void appendEntries(AppendEntriesMessage req, StreamObserver<Response> responseObserver) {
        boolean success = true;
        int term = req.getTerm();
        int prevLogIdx = req.getPrevLogIdx();
        int prevLogTerm = req.getPrevLogTerm();

        if (term < node.getTerm() || !node.prevLogExist(prevLogIdx, prevLogTerm)){ // Recevier implementation 1. & 2. in raft paper pg.4
            success = false;
    
        }

        if (prevLogIdx < node.getLogSize() && node.getLog(prevLogIdx).getTerm() != prevLogTerm){
            node.deleteLog(prevLogIdx);
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
                commands.add(new Command(Integer.valueOf(commandFields.get(i)), Integer.valueOf(commandFields.get(i+1)), commandFields.get(i+2), commandFields.get(i+3)));
            }

            messages.add(new MessageAppendEntries(msg,term, leaderId, prevLogIdx, prevLogTerm, leaderCommitIndex, commands));
        }

        Response reply = Response.newBuilder().setSuccess(success).setTerm(node.getTerm()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
    
    @Override
    public void requestVote(RequestVoteMessage req, StreamObserver<Response> responseObserver) {
        boolean success = false;

        int lli = req.getLastLogIndex();
        int llt = req.getLastLogTerm();
        if(node.getTerm() < req.getTerm())
        {
            Command c = node.getPrevLog(0);

            System.out.println(c.getIndex() + " <= " + lli + ", " + c.getTerm() + "<= "+ llt);
            if(c.getIndex() <= lli && c.getTerm() <= llt)
            {
                success = true;
            }
        }
        System.out.println("Success: " + success);
        if(success)
        {
            String msg = req.getTerm() + " " + req.getCandidateID() + " " + lli + " " + llt;
            messages.add(new MessageRequestVote(msg, req.getTerm(), req.getCandidateID(), lli, llt));
        }
        Response reply = Response.newBuilder().setSuccess(success).setTerm(node.getTerm()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    // This should recive a message from client and it to messages
    // should be able to recieve it as clientMessage and message.poll().log
    // @Override
    public void clientRequest(ClientMessage req, StreamObserver<Empty> responseObserver){
        messages.add(new MessageClient(req.getLog()));
    }


    

    
}
