package lvc.cds.raft;

public class RequestVoteResponse extends Message{

    private String peer;
    private int term;
    private boolean success;

    public RequestVoteResponse(String message, int term, boolean s, String peer)
    {
        super(message, "requestVoteResponse");
        
        this.term = term;
        if(s)
            this.success = true;
        else
            this.success = false;

        this.peer = peer;
        
    }

    public String getPeer()
    {
        return peer;
    }

    public int getTerm()
    {
        return term;
    }

    public boolean getSuccess()
    {
        return success;
    }
    
}
