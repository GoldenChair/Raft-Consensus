package lvc.cds.raft;

public class MessageRequestVote extends Message {
    
    private int term;
    private String candidateId;
    private int lastLogIdx;
    private int lastLogTerm;

    public MessageRequestVote(String msg)
    {
        super(msg, "requestVote");
    }
}
