package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by swati on 4/25/18.
 */

public class Message {

    String msgType;
    String originNode;
    int destNode;

    public Message(){
    }

    public Message(String msgType, String originNode, int destNode) {
        this.msgType = msgType;
        this.originNode = originNode;
        this.destNode = destNode;
    }


//    public Message(String msgType, int predecessor, int successor, int destNode) {
//        this.msgType = msgType;
//        this.destNode = destNode;
//    }

    @Override
    public String toString() {
        return  msgType + "#" + originNode + "#" + destNode;
    }
}
