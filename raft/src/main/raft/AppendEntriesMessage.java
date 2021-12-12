package lvc.cds.raft;

public class AppendEntriesMessage extends Message{
    
    private String[] entries;
    private int term;
    private String leaderId;
    private int prevLogInd;
    private int prevLogTerm;
    private int leaderCommitIndex;

    public AppendEntriesMessage(String message)
    {
        super(message, "appendEntries");
        int index = message.indexOf(" ");
        String subMessage = message.substring(index+1);
        term = Integer.parseInt(message.substring(0, index));

        index = subMessage.indexOf(" ");
        leaderId = subMessage.substring(0,index);
        subMessage = subMessage.substring(index+1);

        index = subMessage.indexOf(" ");
        prevLogInd = Integer.parseInt(subMessage.substring(0,index));
        subMessage = subMessage.substring(index+1);

        index = subMessage.indexOf(" ");
        prevLogTerm = Integer.parseInt(subMessage.substring(0,index));
        subMessage = subMessage.substring(index+1);

        index = subMessage.indexOf("  ");
        String entriesList = subMessage.substring(0,index);
        subMessage = subMessage.substring(index+2);

        leaderCommitIndex = Intger.parse(subMessage);


        //break up entriesList
    }

}
