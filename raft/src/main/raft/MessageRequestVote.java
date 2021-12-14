package lvc.cds.raft;

public class MessageRequestVote extends Message {
    
    private int term;
    private String candidateId;
    private int lastLogIdx;
    private int lastLogTerm;

    public MessageRequestVote(String message)
    {
        super(message, "requestVote");

        int index = message.indexOf(" ");
        String subMessage = message.substring(index+1);
        term = Integer.parseInt(message.substring(0, index));

        index = subMessage.indexOf(" ");
        candidateId = subMessage.substring(0,index);
        subMessage = subMessage.substring(index+1);

        index = subMessage.indexOf(" ");
        lastLogIdx = Integer.parseInt(subMessage.substring(0,index));
        subMessage = subMessage.substring(index+1);

        lastLogTerm = Integer.parseInt(subMessage);
 
    }

    public int getTerm()
    {
        return term;
    }

    public String getCandidateId()
    {
        return candidateId;
    }

    public int getLastLogIdx()
    {
        return lastLogIdx;
    }

    public int getLastLogTerm()
    {
        return lastLogTerm;
    }
}
