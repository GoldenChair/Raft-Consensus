package lvc.cds.raft;

public class RequestVoteResponse extends Message{

    private String peer;
    private int term;
    private boolean success;

    public RequestVoteResponse(String message)
    {
        super(message, "requestVoteResponse");
        
        int index = message.indexOf(" ");
        String subMessage = message.substring(index+1);
        peer = message.substring(0, index);

        index = subMessage.indexOf(" ");
        term = Integer.parseInt(subMessage.substring(0,index));
        subMessage = subMessage.substring(index+1);

        if(subMessage.equals("1"))
        {
            success = true;
        }
        else{
            success = false;
        }
        
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
