package lvc.cds.raft;

public class AppendEntriesResponse extends Message{

    private int term;
    private boolean success;
    private String peer;
    private int size;


    public AppendEntriesResponse(String message, int term, boolean success, String peer, int size)
    {
        super(message, "appendEntriesResponse");
        
        this.peer = peer;
        this.term = term;
        this.success = success;
        this.size = size;
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

    public int getSize()
    {
        return size;
    }
    
}
