package edu.buffalo.cse.cse486586.simpledht;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;

public class SimpleDhtProvider extends ContentProvider {
    static final String REMOTE_PORT0 = "11108";
    static final int SERVER_PORT = 10000;
    static String myPort = "";
    static String portStr = "";
    static String node_id = "";
    static LinkedList<Node> nodeJoiningList = new LinkedList<Node>();
    static String self_node_id = "";
    static String self_node_emu_num = "";
    static String self_node_pred_emu = "";
    static String self_node_succ_emu = "";
    static String firstelem = "";
    static String queryKey = "";
    static String queryVal = "";
    static String flag = "";
    static String globalQueryString = "";
    static String del_flag = "";

    public Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Context context = getContext();
        String hash_selec = "";
        try {
            hash_selec = genHash(selection);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        String hash_pred = "";
        try {
            hash_pred = genHash(self_node_pred_emu);
        }
        catch(Exception e) {
            Log.e("TAG","Hash predecessor exception at insert!");
            e.printStackTrace();
        }

        if(self_node_pred_emu.equals("") && self_node_emu_num.equals("") && self_node_succ_emu.equals("")) {
            if(selection.equals("*") || selection.equals("@")) {
                try {
                    for(int i = 0; i < context.fileList().length; i++) {
                        Log.i("TAG",String.valueOf(context.fileList().length));
                        selection = context.fileList()[i];
                        context.deleteFile(selection);
                       /* BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                        String string = br.readLine();
                        Log.i("TAG","FOUND-->"+string);
                        matrixCursor.addRow(new String[] {selection,string}); */
                    }

                }
                catch (Exception e) {
                    Log.e("TAG","Query failed at first node ");
                    e.printStackTrace();
                }
                //return matrixCursor;
                return 0;
            }
            else {
                try {
                    /*BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                    String string = br.readLine();
                    Log.i("TAG","FOUND-->"+string);
                    matrixCursor.addRow(new String[] {selection,string}); */
                   // selection = context.fileList()[i];
                    context.deleteFile(selection);
                }
                catch(Exception e) {
                    Log.e("TAG","Delete failed at first node ");
                    e.printStackTrace();
                }
                return 0;
                //return matrixCursor;
            }

         }
        else if(!self_node_pred_emu.equals("") && !self_node_emu_num.equals("") && !self_node_succ_emu.equals("")) {

            if(selection.equals("@")) {
                for(int i = 0; i < context.fileList().length; i++) {
                    Log.i("TAG",String.valueOf(context.fileList().length));
                    selection = context.fileList()[i];
                    context.deleteFile(selection);
                       /* BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                        String string = br.readLine();
                        Log.i("TAG","FOUND-->"+string);
                        matrixCursor.addRow(new String[] {selection,string}); */
                }
                return 0;
            }
            else if(selection.equals("*")) {
                del_flag = "true";
                for(int i = 0; i < context.fileList().length; i++) {
                    //Log.i("TAG",String.valueOf(context.fileList().length));
                    selection = context.fileList()[i];
                    context.deleteFile(selection);
                }
                deleteGlobalData(self_node_succ_emu);
                return 0;
            }
            else {
                if(firstelem.compareTo(self_node_id) == 0) {
                    if(hash_selec.compareTo(hash_pred) > 0 || hash_selec.compareTo(self_node_id) < 0) {
                        context.deleteFile(selection);
                        return 0;
                    }
                    else {
                        try {
                            //Log.i("TAG","Forward query to successor when same!!!");
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(self_node_succ_emu)*2);
                            DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                            //Log.i("TAG","FQ server pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                            dout.writeUTF("Delete query"+","+selection);
                            dout.flush();
                            //return 0;
                        }
                        catch(Exception e) {

                        }
                    }
                }
                else {
                    if ((hash_selec.compareTo(hash_pred) > 0 && hash_selec.compareTo(self_node_id) <= 0)) {
                        context.deleteFile(selection);
                        return 0;
                    }
                    else {
                        try {
                        //Log.i("TAG","Forward query to successor when same!!!");
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(self_node_succ_emu) * 2);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        //Log.i("TAG","FQ server pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                        dout.writeUTF("Delete query" + "," + selection);
                        dout.flush();
                         }
                        catch(Exception e) {

                        }
                    }
                }
            }

        }
        return 0;
    }

    public void deleteGlobalData(String succ_num) {
        try {
            Context context = getContext();
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(succ_num)*2);
            DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
            dout.writeUTF("GlobalDeleteQuery");
            dout.flush();

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            String str = dis.readUTF();
            if(str == "Done") {
                return;
            }
        }
        catch(Exception e) {

        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {
            Log.i("TAG","INSERT AT NODE: "+self_node_emu_num);
            Context context = getContext();
            Log.i("TAG","Pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu+" First: "+firstelem+" Node ID: "+self_node_id);
            String key = values.get("key").toString();
            String value = values.get("value").toString();
            Log.i("TAG", "Key value pair to be inserted "+key+" : "+value);
            String hash_key = "";
            try {
                hash_key = genHash(key);
            }
            catch(Exception e) {
                Log.e("TAG","Hash key exception at insert!");
                e.printStackTrace();
            }
            String hash_pred = "";
            try {
                hash_pred = genHash(self_node_pred_emu);
            }
            catch(Exception e) {
                Log.e("TAG","Hash predecessor exception at insert!");
                e.printStackTrace();
            }
            Log.i("TAG","Hashed pred "+hash_pred);
            Log.i("TAG","Hashed key "+hash_key);
            Log.i("TAG","Hashed node id "+self_node_id);
            Log.i("TAG","Hashed firstelem "+firstelem);

            if(self_node_pred_emu.equals("") && self_node_emu_num.equals("") && self_node_succ_emu.equals("")) {
                try {
                    FileOutputStream fos = context.openFileOutput(key, Context.MODE_PRIVATE);
                    fos.write(value.getBytes());
                    Log.i("TAG","Inserted key "+key+"at node "+self_node_emu_num);
                }
                catch (Exception e) {
                    Log.e("TAG","Insert failed at first node ");
                    e.printStackTrace();
                }
            }
            else if(!self_node_pred_emu.equals("") && !self_node_emu_num.equals("") && !self_node_succ_emu.equals("")) {
                try {
                    if(firstelem.compareTo(self_node_id) == 0) {
                        if(hash_key.compareTo(hash_pred) > 0 || hash_key.compareTo(self_node_id) < 0) {
                            FileOutputStream fos = context.openFileOutput(key, Context.MODE_PRIVATE);
                            fos.write(value.getBytes());

                            Log.i("TAG","Inserted at first elem same as self  key "+key+"at node "+self_node_emu_num);
                        }
                        else {
                            Log.i("TAG", "No node here, forwarding Insert to " + self_node_emu_num + " from " + self_node_succ_emu);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Forward insert to client", key + "," + value, String.valueOf(Integer.parseInt(self_node_succ_emu) * 2));
                        }
                    }
                    else {
                        if ((hash_key.compareTo(hash_pred) > 0 && hash_key.compareTo(self_node_id) <= 0)) {
                            //Log.i("TAG", "WHY AM I NEVER HERE??!!");
                            FileOutputStream fos = context.openFileOutput(key, Context.MODE_PRIVATE);
                            fos.write(value.getBytes());
                            Log.i("TAG", "Inserted key at normal and more than pred less than self " + key + "at node " + self_node_emu_num);
                        }
                        else {
                            Log.i("TAG", "No node here, forwarding Insert to " + self_node_emu_num + " from " + self_node_succ_emu);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Forward insert to client", key + "," + value, String.valueOf(Integer.parseInt(self_node_succ_emu) * 2));
                        }
                    }

                }
                catch(Exception e) {
                    Log.e("TAG","Insert failed ");
                    e.printStackTrace();
                }
            }

        }
        catch(Exception e) {
            Log.e("TAG","Insert exception!");
            e.printStackTrace();
        }
//                            Log.i("TAG","Inserted at first elem same as self  key "+key+"at node "+self_node_emu_num);
//                        }
//                     /*   else if(hash_key.compareTo(self_node_id) < 0) {
//                            FileOutputStream fos = context.openFileOutput(key, Context.MODE_PRIVATE);
//                            fos.write(value.getBytes());
//
//                            Log.i("TAG","Inserted first elem same as self and lesser than self key "+key+"at node "+self_node_emu_num);
//                        } */
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            Context context = getContext();
            TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            node_id = genHash(portStr);
            Log.i("TAG","On create" + "," + myPort + "," + portStr + "," + node_id);
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Join", node_id, myPort, portStr);
            return true;
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return false;
        }
        catch (Exception e) {
            Log.i("TAG", "Can't create a ServerSocket");
            e.printStackTrace();
            return false;
        }
        //return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.i("TAG","Reached server");
            Context ctxt = getContext();
            String[] colNames = new String[]{"key","value"};
            MatrixCursor matrixCursor = new MatrixCursor(colNames);
            Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            while (true) {
                try {
                    Socket ss = serverSocket.accept();
                    Log.i("TAG", "Server!!!");
                    DataInputStream dis = new DataInputStream(ss.getInputStream());
                    String message = dis.readUTF();
                    Log.i("TAG","Message---> "+message);
                    String msgArr[] = message.split(",");
                    String msgType = msgArr[0];
                    Log.i("TAG","MSGTYPE "+msgType);
                    DataOutputStream dos = new DataOutputStream(ss.getOutputStream());
                    if (msgType.equals("Join")) {
                        Log.i("TAG","Inside Server Join ");
                        String joiningNodeId = msgArr[1];
                        String joiningPortNum = msgArr[2];
                        String joiningEmuNum = msgArr[3];
                        Node node = new Node(joiningNodeId, joiningEmuNum, joiningPortNum, null, null);
                        Log.i("TAG","Now lets see ");
                        if (nodeJoiningList.isEmpty() || nodeJoiningList.size() == 0) {
                            //Log.i("TAG","Im empty "+nodeJoiningList.size());
                            nodeJoiningList.add(node);
                            firstelem = nodeJoiningList.get(0).joiningNodeId;
                            Log.i("TAG"," pred: "+self_node_pred_emu+" self: "+self_node_emu_num+" succ "+self_node_succ_emu);
                        }
                        else if(nodeJoiningList.size() == 1) {
                            //Log.i("TAG","One elem "+nodeJoiningList.size());
                            nodeJoiningList.add(node);
                            Collections.sort(nodeJoiningList, new Comparator<Node>() {
                                @Override
                                public int compare (Node n1, Node n2) {
                                    if(n1.joiningNodeId.compareTo(n2.joiningNodeId) == 0) {
                                        return 0;
                                    }
                                    else if(n1.joiningNodeId.compareTo(n2.joiningNodeId) > 0) {
                                        return 1;
                                    }
                                    return -1;


                                }
                            });
                            firstelem = nodeJoiningList.get(0).joiningNodeId;
                            Log.i("TAG","at 0th elem "+nodeJoiningList.get(0).joiningEmuNum);
                            Log.i("TAG","at 1st elem "+nodeJoiningList.get(1).joiningEmuNum);
                            Log.i("TAG","First elem "+firstelem);
                            Log.i("TAG"," pred: "+self_node_pred_emu+" self: "+self_node_emu_num+" succ "+self_node_succ_emu);
                            node.predecessor = nodeJoiningList.get(1);
                            node.successor = nodeJoiningList.get(1);
                            nodeJoiningList.get(1).predecessor = node;
                            nodeJoiningList.get(1).successor = node;
                            Log.i("TAG"," pred: "+self_node_pred_emu+" self: "+self_node_emu_num+" succ "+self_node_succ_emu);

                          //  Log.i("TAG",nodeJoiningList.get(0).joiningEmuNum+" "+nodeJoiningList.get(0).predecessor.joiningEmuNum+" "+nodeJoiningList.get(0).successor.joiningEmuNum+" "+node.joiningEmuNum+" "+node.predecessor.joiningEmuNum+" "+node.successor.joiningEmuNum);

                        }
                        else if(nodeJoiningList.size() > 1) {
                            Log.i("TAG","Lots "+nodeJoiningList.size());
                            nodeJoiningList.add(node);
//                            Collections.sort(nodeJoiningList);
                            Collections.sort(nodeJoiningList, new Comparator<Node>() {
                                @Override
                                public int compare (Node n1, Node n2) {
                                    if(n1.joiningNodeId.compareTo(n2.joiningNodeId) == 0) {
                                        return 0;
                                    }
                                    else if(n1.joiningNodeId.compareTo(n2.joiningNodeId) > 0) {
                                        return 1;
                                    }
                                    return -1;


                                }
                            });
                            for (int i = 0; i < nodeJoiningList.size(); i++) {
                                if (i == (nodeJoiningList.size()-1)) {
                                    nodeJoiningList.get(i).successor = nodeJoiningList.get(0);
                                    nodeJoiningList.get(0).predecessor = nodeJoiningList.get(i);
                                    break;
                                }
                                nodeJoiningList.get(i).successor = nodeJoiningList.get(i + 1);
                                nodeJoiningList.get(i + 1).predecessor = nodeJoiningList.get(i);
                            }

                            firstelem = nodeJoiningList.get(0).joiningNodeId;
                            Log.i("TAG","First Elem "+firstelem + ","+ nodeJoiningList.get(0).joiningEmuNum);
                            for (int i = 0; i < nodeJoiningList.size(); i++) {
                                Log.i("TAG","After Display nodeJoiningList--> "+nodeJoiningList.get(i).joiningEmuNum + "," + nodeJoiningList.get(i).predecessor.joiningEmuNum + "," + nodeJoiningList.get(i).successor.joiningEmuNum);
                            }
                            Log.i("TAG"," pred: "+self_node_pred_emu+" self: "+self_node_emu_num+" succ "+self_node_succ_emu);
                        }
                        if(nodeJoiningList.size() != 0 && nodeJoiningList.size() != 1) {
                            sendUpdate();
                        }
                    }
                    else if(msgType.equals("Join done from client")) {
                        Log.i("TAG","FINALLY! "+message);
                        Log.i("TAG","REACHED! BEFORE pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+"Succ: "+self_node_succ_emu);
                        firstelem = msgArr[1];
                        self_node_id = msgArr[2];
                        self_node_emu_num = msgArr[3];
                        self_node_pred_emu = msgArr[4];
                        self_node_succ_emu = msgArr[5];
                        Log.i("TAG","REACHED! AFTER succ "+self_node_succ_emu+"  self: "+self_node_emu_num+" pred: "+self_node_pred_emu);
                    }
                    else if(msgType.equals("Forward insert")) {
                        Log.i("TAG","Inside server forward insert!");
                        Log.i("TAG","FI server pred  "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                        Log.i("TAG","MESSAGE -->"+message);
                        String key = msgArr[1];
                        String value = msgArr[2];
                        ContentValues keyValuePair = new ContentValues();
                        keyValuePair.put("key",key);
                        keyValuePair.put("value",value);
                        insert(providerUri, keyValuePair);
                    }
                    else if(msgType.equals("Forward query")) {
                        Log.i("TAG","Inside server forward query!");
                        Log.i("TAG","FQ server pred  "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                        Log.i("TAG","MESSAGE -->"+message);
                        String selection = msgArr[1];
                        Cursor cq = query(providerUri, null, selection, null, null);
                        if(cq.getCount()==0) {
                            Log.i("TAG","WHEN?????"+queryKey+"   "+queryVal);
                            dos.writeUTF(queryKey+","+queryVal);
                        }
                        else {
                            int keyIndex = cq.getColumnIndex("key");
                            int valueIndex = cq.getColumnIndex("value");
                            cq.moveToFirst();
                            String returnKey = cq.getString(keyIndex);
                            String returnValue = cq.getString(valueIndex);
                            Log.i("TAG","Return value :"+returnValue);
                            dos.writeUTF(returnKey+","+returnValue);
                        }
                    }
                    else if(msgType.equals("From forwardStarQuery")) {
                        Log.i("TAG","SERVER GLOBAL AM I HERE??");
                       // dos.writeUTF(msgArr[1]);
                       // dos.flush();
                        //return msgArr[1];
                        Log.i("TAG","SERVER flag? "+flag);
                        Log.i("TAG","?? "+globalQueryString);
                        for(int i = 1; i < msgArr.length; i++) {
                            if(globalQueryString.equals("")) {
                                globalQueryString = msgArr[i];
                            }
                            else {
                                globalQueryString = globalQueryString + "," + msgArr[i];
                            }

                        }
                        Log.i("TAG","!!! "+globalQueryString);
                        if(flag == "false" || flag == "") {
                            flag = "true";
                            for(int i = 0; i < ctxt.fileList().length; i++) {
                                Log.i("TAG",String.valueOf(ctxt.fileList().length));
                                //selection = context.fileList()[i];
                                BufferedReader br = new BufferedReader(new InputStreamReader(ctxt.openFileInput(ctxt.fileList()[i])));
                                String string = br.readLine();
                                Log.i("TAG","FOUND-->"+string);
                                matrixCursor.addRow(new String[]{ctxt.fileList()[i],string});
                            }
                            if (matrixCursor.getCount() != 0) {
                                matrixCursor.moveToFirst();
                                do {
                                    for (int i = 0; i < matrixCursor.getColumnCount(); i++) {
                                        globalQueryString = globalQueryString + "," + matrixCursor.getString(i);
                                    }
                                }
                                while (matrixCursor.moveToNext());
                            }
                            Log.i("TAG","server globalQueryString----> "+globalQueryString);
                            globalQueryString = forwardStarQuery(globalQueryString, self_node_succ_emu);
                            dos.writeUTF(globalQueryString);
                            dos.flush();
                            globalQueryString = "";
                            flag = "";

                        }
                        else if(flag == "true") {
                            Log.i("TAG","Go back to query cause already done"+globalQueryString);
                            dos.writeUTF(globalQueryString);
                            dos.flush();
                        }

                    }
                    else if(msgType.equals("Delete query")) {
                          Log.i("TAG","Inside server forward insert!");
                          String key = msgArr[1];
                          ctxt.deleteFile(key);

                    }
                    else if(msgType.equals("GlobalDeleteQuery")) {

                        if(del_flag == "")
                        {
                            del_flag = "true";
                            for (int i = 0; i < ctxt.fileList().length; i++) {
                                Log.i("TAG", String.valueOf(ctxt.fileList().length));
                                //selection = context.fileList()[i];
                                BufferedReader br = new BufferedReader(new InputStreamReader(ctxt.openFileInput(ctxt.fileList()[i])));
                                String string = br.readLine();
                                ctxt.deleteFile(string);
                        }
                        deleteGlobalData(self_node_succ_emu);
                        }
                        else {
                            dos.writeUTF("Done");
                            dos.flush();
                        }


                    }

                }
                catch(Exception e) {
                    Log.e("TAG","Server Exception!! ");
                    e.printStackTrace();
                }
            }
            //return null;
        }

    }

    private void sendUpdate() {
        firstelem = nodeJoiningList.get(0).joiningNodeId;
        for (int i = 0; i < nodeJoiningList.size(); i++) {
            Log.i("TAG","forrrrr "+i +" "+nodeJoiningList.size());
            Log.i("TAG","Node ID: "+nodeJoiningList.get(i).joiningNodeId+" First elem: "+firstelem);
            Log.i("TAG","! succ "+nodeJoiningList.get(i).successor.joiningEmuNum+"  self: "+nodeJoiningList.get(i).joiningEmuNum+" pred: "+nodeJoiningList.get(i).predecessor.joiningEmuNum);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Join done from port 0 ", firstelem, nodeJoiningList.get(i).joiningNodeId, nodeJoiningList.get(i).joiningEmuNum,nodeJoiningList.get(i).predecessor.joiningEmuNum, nodeJoiningList.get(i).successor.joiningEmuNum);
        }
    }

    public String forwardStarQuery(String globalQueryString, String p) {
        try {
            Log.i("TAG","Inside forwardStarQuery");
            Log.i("TAG","will connect to "+p);
            Log.i("TAG"," globalQueryString--->  "+globalQueryString);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(p)*2);
            DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
            dout.writeUTF("From forwardStarQuery"+","+globalQueryString);
            Log.i("TAG",globalQueryString);
            dout.flush();

            DataInputStream dis = new DataInputStream(socket.getInputStream());
            globalQueryString = dis.readUTF();
            Log.i("TAG","globalQueryString at node "+self_node_emu_num+" "+globalQueryString);
           /* if(globalQueryString == "Done") {

            }*/

        } catch (IOException e) {
            e.printStackTrace();
        }
        return globalQueryString;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        String str="";

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String msgType = msgs[0];
                Log.i("TAG", "Reached ClientTask ");
                String[] colNames = new String[]{"key","value"};
                MatrixCursor matrixCursor = new MatrixCursor(colNames);
                if(msgType.equals("Join")) {
                    Log.i("TAG","Client Task");
                    node_id = msgs[1];
                    myPort = msgs[2];
                    portStr = msgs[3];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT0));
                    String msgToSend = msgType + "," + node_id + "," + myPort + "," + portStr;
                    Log.i("TAG","Message to send "+msgToSend);
                    //   DataInputStream dIS = new DataInputStream(socket.getInputStream());
                    DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
                    dOut.writeUTF(msgToSend);
                    Log.i("TAG","Message Sent");
                    dOut.flush();
                }
                else if(msgType.equals("Join done from port 0 ")) {
                    Log.i("TAG","Client Task Join done");
                    firstelem = msgs[1];
                    String nodeToSend = msgs[2];
                    String nodeToSend_Emu  = msgs[3];
                    String nodeToSend_pred_Emu = msgs[4];
                    String nodeToSend_succ_Emu = msgs[5];
                    //Log.i("TAG","Size   "+nodeJoiningList.size());
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nodeToSend_Emu)*2);
                    DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
                    //Log.i("TAG","Size ag  "+nodeJoiningList.size());
                    String msgToSend = "Join done from client"+ "," + firstelem + "," + nodeToSend + "," + nodeToSend_Emu + "," + nodeToSend_pred_Emu+ "," + nodeToSend_succ_Emu;
                    Log.i("TAG","Client Task Join done msgToSend--->" + msgToSend);
                    dOut.writeUTF(msgToSend);
                    dOut.flush();
                }
                else if(msgType.equals("Forward insert to client")) {
                    try {
                        Log.i("TAG","Forward insert to client !!!");
                        String key_value = msgs[1];
                        String key_value_arr[] = key_value.split(",");
                        String key = key_value_arr[0];
                        String value = key_value_arr[1];
                        String forward_port = msgs[2];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(forward_port));
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        Log.i("TAG","FI server pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                        Log.i("TAG","Forwarding insert from client to server port num "+forward_port);
                        dout.writeUTF("Forward insert"+","+key+ "," + value);
                        dout.flush();
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
                //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Forward query to client",selection,String.valueOf(Integer.parseInt(self_node_succ_emu)*2));
               /* else if(msgType.equals("Forward query to client")) {
                    try{
                        Log.i("TAG","Forward query to client !!!");
                        String selection = msgs[1];
                        String forward_port = msgs[2];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(forward_port));
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        Log.i("TAG","FQ server pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                        dout.writeUTF("Forward query"+","+selection);
                        dout.flush();
                        DataInputStream dis = new DataInputStream(socket.getInputStream());
                        String finals = dis.readUTF();
                        String finalsrray[] = finals.split(",");

                        matrixCursor.addRow(new String[] {finalsrray[0], finalsrray[1]});

                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }

                } */

            }
            catch(Exception e) {
                e.printStackTrace();
                return null;
            }
           // return null;
            System.out.println(str);
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,String sortOrder) {
        Log.i("TAG","QUERY!" +selection);
        String[] colNames = new String[]{"key","value"};
        MatrixCursor matrixCursor = new MatrixCursor(colNames);
        Context context = getContext();
        String hash_selec = "";
        try {
            hash_selec = genHash(selection);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        String hash_pred = "";
        try {
            hash_pred = genHash(self_node_pred_emu);
        }
        catch(Exception e) {
            Log.e("TAG","Hash predecessor exception at insert!");
            e.printStackTrace();
        }
        Log.i("TAG","Hashed pred "+hash_pred);
        Log.i("TAG","Hashed node id "+self_node_id);
        Log.i("TAG","Hashed firstelem "+firstelem);
        Log.i("TAG", "Pred--> "+self_node_pred_emu+" Self--> "+self_node_emu_num+" Succ--> "+self_node_succ_emu);
        if(self_node_pred_emu.equals("") && self_node_emu_num.equals("") && self_node_succ_emu.equals("")) {
            if(selection.equals("*") || selection.equals("@")) {
                try {
                    for(int i = 0; i < context.fileList().length; i++) {
                        Log.i("TAG",String.valueOf(context.fileList().length));
                        selection = context.fileList()[i];
                        BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                        String string = br.readLine();
                        Log.i("TAG","FOUND-->"+string);
                        matrixCursor.addRow(new String[] {selection,string});
                    }

                }
                catch (Exception e) {
                    Log.e("TAG","Query failed at first node ");
                    e.printStackTrace();
                }
                return matrixCursor;
            }
            else {
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                    String string = br.readLine();
                    Log.i("TAG","FOUND-->"+string);
                    matrixCursor.addRow(new String[] {selection,string});
                }
                catch(Exception e) {
                    Log.e("TAG","Query failed at first node ");
                    e.printStackTrace();
                }
                return matrixCursor;
            }
        }
        else if(!self_node_pred_emu.equals("") && !self_node_emu_num.equals("") && !self_node_succ_emu.equals("")) {
            if(selection.equals("@")) {
                try {
                    for(int i = 0; i < context.fileList().length; i++) {
                        selection = context.fileList()[i];
                        BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                        String string = br.readLine();
                        Log.i("TAG","FOUND-->"+string);
                        matrixCursor.addRow(new String[] {selection,string});
                    }

                }
                catch (Exception e) {
                    Log.e("TAG","Query failed at first node ");
                    e.printStackTrace();
                }
                return matrixCursor;
            }
            else if(selection.equals("*")) {
                try {
                    //Context context = getContext();
                    Log.i("TAG","FLAG--->"+flag);
                    flag = "true";
                    Log.i("TAG","FLAG after setting at node--->"+flag + " "+self_node_emu_num);
                    for (int i = 0; i < context.fileList().length; i++) {
                        selection = context.fileList()[i];
                        //Context context = getContext();
                        BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                        String string = br.readLine();
                        //Log.i("TAG", "FOUND-->" + string);
                        matrixCursor.addRow(new String[]{selection, string});
                    }
                    Log.i("TAG", "matrixCursor.getCount()----> " + matrixCursor.getCount());
                    if (matrixCursor.getCount() != 0) {
                        matrixCursor.moveToFirst();
                        do {
                            for (int i = 0; i < matrixCursor.getColumnCount(); i++) {

                                Log.i("TAG", "Before globalQueryString-->" + globalQueryString);
                                Log.i("TAG", "matrixCursor.getString(i)-->" + matrixCursor.getString(i));
                                if(globalQueryString.equals("")) {
                                    globalQueryString = matrixCursor.getString(i);
                                }
                                else {
                                    globalQueryString = globalQueryString + "," + matrixCursor.getString(i);
                                }
                                Log.i("TAG", "After globalQueryString-->" + globalQueryString);
                            }
                        } while (matrixCursor.moveToNext());
                    }
                    Log.i("TAG", "After adding self node globalQueryString-->" + globalQueryString);

                    String str = forwardStarQuery(globalQueryString,self_node_succ_emu);
                    Log.i("TAG","STR--> "+str);
                    String[] strArr = str.split(",");
                    Log.i("TAG","STR LENGTH--> "+strArr.length);
                    for(int i=0;i < strArr.length; i = i+2) {
                        matrixCursor.addRow(new String[] {strArr[i],strArr[i+1]});
                        //Log.i("TAG","FINAL FOR LOOP---->");
                       // Log.i("TAG",strArr[i]);
                    }

                    globalQueryString = "";
                    flag = "";
                   // Log.i("TAG","YAYYYYYYYYYYYY---->"+str);
                    return matrixCursor;

                 }
                catch (Exception e) {
                    Log.e("TAG","Query failed at first node ");
                    e.printStackTrace();
                }

            }
            else {
                try {
                    if(firstelem.compareTo(self_node_id) == 0) {
                        if(hash_selec.compareTo(hash_pred) > 0 || hash_selec.compareTo(self_node_id) < 0) {
                            BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                            String string = br.readLine();
                            Log.i("TAG", "FOUND-->" + string);
                            matrixCursor.addRow(new String[]{selection, string});
                            return matrixCursor;
                        }
                        else {
                            Log.i("TAG","Forward query to successor when same!!!");
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(self_node_succ_emu)*2);
                            DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                            Log.i("TAG","FQ server pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                            dout.writeUTF("Forward query"+","+selection);
                            dout.flush();
                            DataInputStream dis = new DataInputStream(socket.getInputStream());
                            String string = dis.readUTF();
                            Log.i("TAG","String--->"+string);
                            String stringArr[] = string.split(",");
                            String key = stringArr[0];
                            String value = stringArr[1];
                            matrixCursor.addRow(new String[] {key,value});
                            return matrixCursor;
                        }
                    }
                    else {
                        if ((hash_selec.compareTo(hash_pred) > 0 && hash_selec.compareTo(self_node_id) <= 0)) {
                            BufferedReader br = new BufferedReader(new InputStreamReader(context.openFileInput(selection)));
                            String string = br.readLine();
                            Log.i("TAG", "FOUND-->" + string);
                            matrixCursor.addRow(new String[]{selection, string});
                            return matrixCursor;
                        }
                        else {
                            Log.i("TAG","Forward query to successor !!!");
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(self_node_succ_emu)*2);
                            DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                            Log.i("TAG","FQ server pred: "+self_node_pred_emu+" Self: "+self_node_emu_num+" Succ: "+self_node_succ_emu);
                            dout.writeUTF("Forward query"+","+selection);
                            dout.flush();
                            DataInputStream dis = new DataInputStream(socket.getInputStream());
                            String string = dis.readUTF();
                            String stringArr[] = string.split(",");
                            matrixCursor.addRow(new String[] {stringArr[0], stringArr[1]});
                            return matrixCursor;
                        }
                    }

                }
                catch(Exception e) {
                    Log.e("TAG","Query failed ");
                    e.printStackTrace();
                }
                return matrixCursor;
            }

    }
        return matrixCursor;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}