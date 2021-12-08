package lvc.cds.raft;

public class Command {
    private int term;
    private String method;
    private String body;


    public Command(int term, String method, String body)
    {
        this.term = term;
        this.method = method;
        this.body = body;
    }

    
}
