package edu.buffalo.cse.cse486586.simpledht;
import java.util.Comparator;
import android.util.Log;

public class Node implements Comparable<Node> {
    String joiningNodeId = "";
    String joiningEmuNum = "";
    String joiningPortNum = "";
    Node predecessor = null;
    Node successor = null;

    Node() {

    }

    Node(String joiningNodeId, String joiningEmuNum, String joiningPortNum, Node predecessor, Node successor){
        this.joiningNodeId = joiningNodeId;
        this.joiningEmuNum = joiningEmuNum;
        this.joiningPortNum = joiningPortNum;
        this.predecessor = predecessor;
        this.successor = successor;
    }

    @Override
    public int compareTo (Node n1) {
        if(this.joiningNodeId.compareTo(n1.joiningNodeId) == 0) {
            return 0;
        }
        else if(this.joiningNodeId.compareTo(n1.joiningNodeId) > 0) {
            return 1;
        }
        return -1;

     /*   if(this.joiningEmuNum.compareTo(n1.joiningEmuNum) == 0) {
            return 0;
        }
        else if(this.joiningEmuNum.compareTo(n1.joiningEmuNum) > 0) {
            return 1;
        }
        return -1; */

    }

}