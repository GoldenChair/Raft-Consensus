package lvc.cds.raft;

import java.util.ArrayList;

public class MessageAppendEntries extends Message{
    
    private int term;
    private String leaderId;
    private int prevLogIdx;
    private int prevLogTerm;
    private int leaderCommitIndex;
    private ArrayList<Command> entries;


    public MessageAppendEntries(String message, int term, String leaderid, int prevLogIdx, int prevLogTerm, int leaderCommitIndex, ArrayList<Command> commands)
    {
        super(message, "appendEntries");
        this.term = term;
        this.leaderId = leaderid;
        this.prevLogIdx = prevLogIdx;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommitIndex = leaderCommitIndex;
        this.entries = commands;
    }

    public int getTerm()
    {
        return term;
    }

    public String getLeaderId()
    {
        return leaderId;
    }

    public int getPrevLogIdx()
    {
        return prevLogIdx;
    }

    public int getPrevLogTerm()
    {
        return prevLogTerm;
    }

    public ArrayList<Command> getAllEntries()
    {
        return entries;
    }

    public int getLeaderCommitIndex()
    {
        return leaderCommitIndex;
    }

}
