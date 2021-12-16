package lvc.cds.raft;

public class Message {
    private String type;
    private String msg;

    Message(String msg, String type) {
        this.msg = msg;
        this.type = type;
    }

    public String getMsg()
    {
        return msg;
    }

    public String getType()
    {
        return type;
    }
}

