package lvc.cds.raft;



public static class PeerStub {
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

    void sendAppendEntries(int term, String leaderId, int prevLogIdx, int prevLogTerm, AttayList<String> entries, int leaderCommitIndex) {
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
                messages.add(new Message(msg));
            }

            @Override
            public void onError(Throwable t) {
                messages.add(new Message("error handling response"));
            }

            @Override
            public void onCompleted() {
                System.err.println("stream observer onCompleted");
            }
        });


    }

    void sendRequestVote() {

    }
}
