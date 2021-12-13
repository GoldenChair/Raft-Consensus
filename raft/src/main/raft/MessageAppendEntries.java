package lvc.cds.raft;

public class MessageAppendEntries extends Message{
    
    private int term;
    private String leaderId;
    private int prevLogIdx;
    private int prevLogTerm;
    private int leaderCommitIndex;
    private ArrayList<Command> entries;


    public MessageAppendEntries(String message)
    {
        super(message, "appendEntries");
        int index = message.indexOf(" ");
        String subMessage = message.substring(index+1);
        term = Integer.parseInt(message.substring(0, index));

        index = subMessage.indexOf(" ");
        leaderId = subMessage.substring(0,index);
        subMessage = subMessage.substring(index+1);

        index = subMessage.indexOf(" ");
        prevLogIdx = Integer.parseInt(subMessage.substring(0,index));
        subMessage = subMessage.substring(index+1);

        index = subMessage.indexOf(":");
        prevLogTerm = Integer.parseInt(subMessage.substring(0,index));
        subMessage = subMessage.substring(index+1);

        index = subMessage.indexOf("  ");
        String entriesList = subMessage.substring(0,index);
        subMessage = subMessage.substring(index+2);

        index = subMessage.indexOf(" ");
        leaderCommitIndex = Intger.parse(subMessage.substring(0, index));
        subMessage = subMessage.substring(index+1);
    

        int t;
        int i;
        String body;
        String method;
        //break up entriesList
        while(entriesList.length() > 0)
        {
            index = entriesList.indexOf(":");
            i = Integer.parseInt(entriesList.substring(0, index));
            entriesList = entriesList.substring(index+1);

            index = entriesList.indexOf(":");
            i = Integer.parseInt(entriesList.substring(0, index));
            entriesList = entriesList.substring(index+1);

            index = entriesList.indexOf(":");
            i = Integer.parseInt(entriesList.substring(0, index));
            entriesList = entriesList.substring(index+1);

            index = entriesList.indexOf(":");
            if(index > -1)
                body = Integer.parseInt(entriesList.substring(0, index));
            entriesList = entriesList.substring(index+1);


            entries.add(new Command(t, i, method, body));
        }
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

    public ArrayList<String> getAllEntries()
    {
        return entries;
    }

    public int getLeaderCommitIndex()
    {
        return leaderCommitIndex;
    }

}
