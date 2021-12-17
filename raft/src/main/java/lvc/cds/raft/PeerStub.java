package lvc.cds.raft;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.AppendEntriesMessage;
import lvc.cds.raft.proto.RaftRPCGrpc;
import lvc.cds.raft.proto.RequestVoteMessage;
import lvc.cds.raft.proto.Response;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCStub;

public class PeerStub {
    ConcurrentLinkedQueue<Message> messages;
    String peer;
    int port;

    ManagedChannel channel;
    RaftRPCStub stub;

    PeerStub(String peer, int port, ConcurrentLinkedQueue<Message> messages) {
        this.peer = peer;
        this.port = port;
        this.channel = null;
        this.stub = null;
        this.messages = messages;
    }

    void connect() {
        channel = ManagedChannelBuilder.forAddress(peer, port).usePlaintext().build();
        stub = RaftRPCGrpc.newStub(channel);
    }

    RaftRPCStub getStub() {
        return stub;
    }

    void shutdown() throws InterruptedException {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    void sendAppendEntries(int term, String leaderId, int prevLogIdx, int prevLogTerm, ArrayList<String> entries, int leaderCommitIndex) {
        AppendEntriesMessage request = AppendEntriesMessage.newBuilder()
        .setTerm(term).setLeaderID(leaderId)
        .setPrevLogIdx(prevLogIdx).setPrevLogTerm(prevLogTerm)
        .addAllEntries(entries)
        .setLeaderCommitIdx(leaderCommitIndex)
        .build();

        getStub().appendEntries(request, new StreamObserver<Response>() {
            @Override
            public void onNext(Response value) {
                // we have the peer string (IP address) available here.
                String msg = "reply from " + peer + " " + value.getTerm() + " " + value.getSuccess();
                messages.add(new AppendEntriesResponse(msg, value.getTerm(), value.getSuccess(), peer, entries.size()/4), prevLogIdx);
            }

            @Override
            public void onError(Throwable t) {
                messages.add(new Message("error","error handling response"));
            }

            @Override
            public void onCompleted() {
                System.err.println("stream observer onCompleted");
            }
        });


    }

    void sendRequestVote(int term, String candidateId, int lastLogIdx, int lastLogTerm) {
        RequestVoteMessage request = RequestVoteMessage.newBuilder()
        .setTerm(term).setCandidateID(candidateId)
        .setLastLogIndex(lastLogIdx).setLastLogTerm(lastLogTerm)
        .build();

        getStub().requestVote(request, new StreamObserver<Response>() {
            @Override
            public void onNext(Response value) {
                // we have the peer string (IP address) available here.
                String msg = "" + peer + " " + value.getTerm() + " " + value.getSuccess();
                messages.add(new RequestVoteResponse(msg, value.getTerm(), value.getSuccess(), peer));
            }

            @Override
            public void onError(Throwable t) {
                messages.add(new Message("error", "error handling response"));
            }

            @Override
            public void onCompleted() {
                System.err.println("stream observer onCompleted");
            }
        });
    }
}
