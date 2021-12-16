package lvc.cds.raft;

public class MessageRequestVote extends Message {
    
    private int term;
    private String candidateId;
    private int lastLogIdx;
    private int lastLogTerm;

    public MessageRequestVote(String message, int term, String id, int lastIdx, int lastTerm)
    {
        super(message, "requestVote");

        this.term = term;
        candidateId = id;
        lastLogIdx = lastIdx;
        lastLogTerm = lastTerm;
 
    }

    public int getTerm()
    {
        return term;
    }

    public String getCandidateId()
    {
        return candidateId;
    }

    public int getLastLogIdx()
    {
        return lastLogIdx;
    }

    public int getLastLogTerm()
    {
        return lastLogTerm;
    }
}
