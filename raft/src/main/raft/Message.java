package lvc.cds.raft;

public class Message {
    String type;
    String msg;

    Message(String msg, String type) {
        this.msg = msg;
    }
}

