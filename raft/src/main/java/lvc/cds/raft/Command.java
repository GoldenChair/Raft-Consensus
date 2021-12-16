package lvc.cds.raft;

public class Command {
    private int term;
    private int index;
    private String method;
    private String body;



    public Command(int term, int index, String method, String body)
    {
        this.term = term;
        this.index = index;
        this.method = method;
        this.body = body;
    }

    public int getTerm()
    {
        return term;
    }

    public int getIndex()
    {
        return index;
    }

    public String getMethod()
    {
        return method;
    }

    public String getBody()
    {
        return body;
    }

    
}
