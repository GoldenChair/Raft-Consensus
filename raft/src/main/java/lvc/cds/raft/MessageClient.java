package lvc.cds.raft;

public class MessageClient extends Message {
    
    private String clientRequest;

    public MessageClient(String message)
    {
        super(message, "client");

        clientRequest = message;
 
    }

    public String getClientRequest()
    {
        return clientRequest;
    }
}
