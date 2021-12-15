package lvc.cds.raft;

public class AppendEntriesResponse extends Message{

    private int term;
    private boolean success;
    private String peer;


    public AppendEntriesResponse(String message, int term, boolean success, String peer)
    {
        super(message, "appendEntriesResponse");
        
        this.peer = peer;
        this.term = term;
        this.success = success;
        
    }


    public int getTerm()
    {
        return term;
    }

    public boolean getSuccess()
    {
        return success;
    }

    public String getPeer()
    {
        return peer;
    }
    
}
