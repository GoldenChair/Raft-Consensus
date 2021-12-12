package lvc.cds.raft;

public class RequestVoteMessage extends Message {
    
    private int term;
    private String candidateId;
    private int lastLogIdx;
    private int lastLogTerm;

    public RequestVoteMessage(String msg)
    {
        super(msg, "requestVote");
    }
}
