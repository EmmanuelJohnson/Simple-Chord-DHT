package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final int [] allPorts = new int[]{11108, 11112, 11116, 11120, 11124};
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    static final int SUPER_NODE = 5554;
    static final int SUPER_NODE_PORT = 11108;

    private static final List<Node> chordRing = new ArrayList<Node> ();

    //From PA2A OnPTestClickListener.java File
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    static String myPort = null;
    static String myAvd = null;
    static Node myNode = null;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v("delete", selection);
        if(selection.equals("@")){
            //Delete all files in this node
            Log.d("delete", "inside @");
            String[] fileList = getContext().fileList();
            for (int i = 0; i < fileList.length; i++) {
                deleteFile(fileList[i]);
            }
        }
        else if(selection.equals("*")){
            //Delete all files from all nodes
            Log.d("delete", "inside *");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "", myPort, myAvd, "DeleteAllFiles", "multi");
        }
        else{
            String keyHash = getKeyHash(selection);
            if(myNode.getPredecessor() != null){//It means it is not the only AVD in the chord ring
                if((Integer.parseInt(myAvd) == SUPER_NODE && chordRing.size() == 1) || isBelongToMyNode(keyHash)) {
                    deleteFile(selection);
                }
                else{
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash+":"+selection, myPort, myAvd, "FindAndDeleteAtNode", "uni");
                }
            }
            else{
                deleteFile(selection);
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //From PA2A
        Log.v("insert", values.toString());
        String value = (String) values.get("value");
        String key = (String) values.get("key");
        String keyHash = getKeyHash(key);
        if(myNode.getPredecessor() != null) {//It means it is not the only AVD in the chord ring
            if ((Integer.parseInt(myAvd) == SUPER_NODE && chordRing.size() == 1) || isBelongToMyNode(keyHash)) {
                saveFile(key, value);
            } else {
                Log.d("check", "look for node: "+key);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash + ":" + key + ":" + value, myPort, myAvd, "GetNodeLocationInsert", "uni");
            }
        }
        else{
            saveFile(key, value);
        }

        return uri;
    }

    @Override
    public boolean onCreate() {
        //From PA1
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myAvd = portStr;
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d(TAG, "onCreate: "+myPort);
        myNode = new Node(Integer.parseInt(myAvd), Integer.parseInt(myPort));//Create a node object for the current AVD
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.d(TAG, "onCreate: initialized server socket");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);//Creating the async task
            Log.d(TAG, "onCreate: initialized a new server task");
        } catch (IOException e) {
            Log.e(TAG, "onCreate: ", e);
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        if(Integer.parseInt(myAvd) == SUPER_NODE){//Check if the current avd is the super avd/node (Master Node)
            Node superNode = new Node(SUPER_NODE, SUPER_NODE_PORT);
            chordRing.add(superNode);//Add it to our chord ring. No need to send node join request to itself.
        }
        else{//Send node join request to avd/node 5554 in port 11108
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "", myPort, myAvd, "NodeJoinRequest", "uni");
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        //From PA2A
        Log.v("query", selection);                      // The sort order for the returned rows
        //https://stackoverflow.com/questions/18290864/create-a-cursor-from-hardcoded-array-instead-of-db
        String[] columns = new String[] { "key", "value" };
        //https://developer.android.com/reference/android/database/MatrixCursor.html#MatrixCursor(java.lang.String[])
        MatrixCursor cursor = new MatrixCursor(columns);
        FileInputStream inputStream;
        if(selection.equals("@")){
            //Get all files in this node
            Log.d("query", "inside @");
            for (int i = 0; i < getContext().fileList().length; i++) {
                try {
                    inputStream = getContext().openFileInput(getContext().fileList()[i]);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
                    String line = reader.readLine();
                    cursor.addRow(new Object[] {getContext().fileList()[i], line});
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return cursor;
        }
        else if(selection.equals("*")){
            //Get all files from all nodes
            Log.d("query", "inside *");
            int nDevices = 0;
            Message msgObject = new Message("", Integer.parseInt(myPort), Integer.parseInt(myAvd), "GetAllFiles", 1);
            while (nDevices < 5) {
                try {
                    int remotePort = allPorts[nDevices];
                    msgObject.setToPort(remotePort);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
                    //https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
                    //https://www.careerbless.com/samplecodes/java/beginners/socket/SocketBasic1.php
                    OutputStream msgOpStrm = socket.getOutputStream();
                    ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
                    sendOp.writeObject(msgObject);
                    sendOp.flush();
                    //socket.close();
                    Message gafRecMsg = null;
                    InputStream msgInStrm = socket.getInputStream();
                    ObjectInputStream recInp = new ObjectInputStream(msgInStrm);
                    gafRecMsg = (Message) recInp.readObject();
                    for (int i = 0; i < gafRecMsg.getValList().size(); i++) {
                        String keyValPair = gafRecMsg.getValList().get(i).toString();
                        cursor.addRow(new Object[] {keyValPair.split(":")[0], keyValPair.split(":")[1]});
                    }
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "Query * UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "Query * socket IOException" + e);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                nDevices += 1;
            }
            return cursor;
        }
        else{
            String keyHash = getKeyHash(selection);
            if(myNode.getPredecessor() != null){//It means it is not the only AVD in the chord ring
                if((Integer.parseInt(myAvd) == SUPER_NODE && chordRing.size() == 1) || isBelongToMyNode(keyHash)) {
                    try {
                        inputStream = getContext().openFileInput(selection);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
                        String line = reader.readLine();
                        cursor.addRow(new Object[] {selection, line});
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return cursor;
                }
                else{
                    Message msgObject = new Message(keyHash+":"+selection, Integer.parseInt(myPort), Integer.parseInt(myAvd), "GetNodeLocationQuery", 7);
                    try {
                        msgObject.setToPort(SUPER_NODE_PORT);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), SUPER_NODE_PORT);
                        //https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
                        OutputStream msgOpStrm = socket.getOutputStream();
                        ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
                        sendOp.writeObject(msgObject);
                        sendOp.flush();
                        //socket.close();
                        Message qRecMsg = null;
                        InputStream msgInStrm = socket.getInputStream();
                        ObjectInputStream recInp = new ObjectInputStream(msgInStrm);
                        qRecMsg = (Message) recInp.readObject();
                        socket.close();

                        Message msgQObject = new Message(keyHash+":"+selection, Integer.parseInt(myPort), Integer.parseInt(myAvd), "GetQueryAtNode", 8);
                        msgQObject.setToPort(qRecMsg.getNode().getPort());
                        Socket qSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msgQObject.getToPort());
                        //https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
                        OutputStream msgQOpStrm = qSocket.getOutputStream();
                        ObjectOutputStream sendQOp = new ObjectOutputStream(msgQOpStrm);
                        sendQOp.writeObject(msgQObject);
                        sendQOp.flush();
                        //socket.close();
                        Message finalQMsg = null;
                        InputStream fMsgInStrm = qSocket.getInputStream();
                        ObjectInputStream recQInp = new ObjectInputStream(fMsgInStrm);
                        finalQMsg = (Message) recQInp.readObject();
                        cursor.addRow(new Object[] {finalQMsg.getMsg().split(":")[1], finalQMsg.getMsg().split(":")[2]});
                        qSocket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "Query * UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "Query * socket IOException" + e);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    return cursor;
                }
            }
            else{//Handle only one node in the chord ring
                Log.d(TAG, "query: inside selection");
                try {
                    inputStream = getContext().openFileInput(selection);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
                    String line = reader.readLine();
                    cursor.addRow(new Object[] {selection, line});
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return cursor;
            }
        }
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

    //From PA1
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while(true) {
                    Socket socket = serverSocket.accept();
                    Message recInp = (Message) (new ObjectInputStream(socket.getInputStream())).readObject();
                    String recMsg = String.valueOf(recInp.getMsg());
                    Log.d(TAG, "message recieved: "+recMsg);
                    int msgTypeCode = recInp.getTypeCode();
                    switch (msgTypeCode){
                        case 1:
                            //Create a value list and store all key value pairs in the current node and send it back to origin of request
                            List<String> cVals = new ArrayList<String>();
                            try {
                                Cursor resultCursor = query(mUri, null, "@", null, null);
                                if (resultCursor == null) {
                                    Log.e(TAG, "Result null");
                                    throw new Exception();
                                }
                                resultCursor.moveToFirst();
                                for (int i = 0; i < resultCursor.getCount(); i++) {
                                    String strReceived = resultCursor.getString(resultCursor.getColumnIndex(KEY_FIELD)) + ":"
                                            + resultCursor.getString(resultCursor.getColumnIndex(VALUE_FIELD));
                                    cVals.add(strReceived);
                                    resultCursor.moveToNext();
                                }
                            }
                            catch (Exception e) {
                                Log.e("serverError", "Case 1 Server Error: "+e);
                            }
                            Message respMsgObj = new Message("GAF-ACK", Integer.parseInt(myPort), Integer.parseInt(myAvd), "RespAllFiles", 2);
                            respMsgObj.setValList(cVals);
                            respMsgObj.setToPort(recInp.getFromPort());

                            OutputStream respMsgOpStrm = socket.getOutputStream();
                            ObjectOutputStream respSendOp = new ObjectOutputStream(respMsgOpStrm);
                            respSendOp.writeObject(respMsgObj);
                            respSendOp.flush();
                            break;
                        case 2:
                            break;
                        case 3:
                            Node reqNode = recInp.getNode();
                            //When a new node join request is received, add the node to the chordRing list and sort it
                            chordRing.add(reqNode);
                            Collections.sort(chordRing);
                            Log.d("nodejoin", "received request");
                            //Iterate through the chordRing list and find predecessor, successor value for each node and
                            //send the values back to that node each time when a node join request is received
                            for (int i = 0; i < chordRing.size(); i++) {
                                Node cNode = chordRing.get(i);
                                if(i == 0){//If Node is at index 0, the predecessor is the last Node in the list
                                    cNode.setPredecessor(chordRing.get(chordRing.size()-1).getHashValue());
                                    if(chordRing.size() == 1)
                                        cNode.setSuccessor(chordRing.get(0).getHashValue());
                                    else
                                        cNode.setSuccessor(chordRing.get(i+1).getHashValue());
                                }
                                else if(i > 0 && i < chordRing.size()-1){
                                    cNode.setPredecessor(chordRing.get(i-1).getHashValue());
                                    cNode.setSuccessor(chordRing.get(i+1).getHashValue());
                                }
                                else{//If Node is at last index, the successor is the first Node in the list
                                    cNode.setPredecessor(chordRing.get(i-1).getHashValue());
                                    cNode.setSuccessor(chordRing.get(0).getHashValue());
                                }
                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, cNode.toString(), myPort, myAvd, "NodeJoinResponse", "uni");
                            }
                            if(Integer.parseInt(myAvd) == SUPER_NODE){//Debug
                                Log.d("ChordRing", "###########");
                                for (int i = 0; i < chordRing.size(); i++) {
                                    Log.d("ChordRing", "index:"+i+", "+chordRing.get(i).toString());
                                }
                            }
                            break;
                        case 4:
                            Node resNode = recInp.getNode();//Received updated pred and succ value for the node
                            myNode = resNode;//Update your current Node
                            Log.d("nodejoin", "node: "+myNode.toString());
                            break;
                        case 5:
                            String[] rSplit = recInp.getMsg().split(":");
                            //To find the partition location for a key, create a temporary node and set its hash value to the key's hash value
                            //Add the temp node to chordRing list and sort it. Find its position in the list, the node after the position of
                            //temp node is the target node.
                            Node tmpNode = new Node(recInp.getAvd(), recInp.getFromPort());
                            tmpNode.setHashValue(rSplit[0]);
                            chordRing.add(tmpNode);
                            Collections.sort(chordRing);
                            int targetIndex;
                            if(chordRing.indexOf(tmpNode) == chordRing.size()-1){//If the temp node is in last position, the next node is the first node (circle/ring)
                                targetIndex = 0;
                            }
                            else{
                                targetIndex = chordRing.indexOf(tmpNode) + 1;
                            }
                            Node targetNode = chordRing.get(targetIndex);
                            chordRing.remove(tmpNode);//Remove the temporary node
                            Collections.sort(chordRing);//Sort the chordRing list again to maintain the partitioning
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recInp.getMsg()+"#"+targetNode.toString(), myPort, myAvd, "InsertAtNode", "uni");
                            break;
                        case 6:
                            //Save the given key value pairs
                            String key = recInp.getMsg().split(":")[1];
                            String value = recInp.getMsg().split(":")[2];
                            saveFile(key, value);
                            break;
                        case 7:
                            //Similar to case 5
                            String[] rQSplit = recInp.getMsg().split(":");
                            Node tmpQNode = new Node(recInp.getAvd(), recInp.getFromPort());
                            tmpQNode.setHashValue(rQSplit[0]);
                            chordRing.add(tmpQNode);
                            Collections.sort(chordRing);
                            int targetQIndex;
                            if(chordRing.indexOf(tmpQNode) == chordRing.size()-1){
                                targetQIndex = 0;
                            }
                            else{
                                targetQIndex = chordRing.indexOf(tmpQNode) + 1;
                            }
                            Node targetQNode = chordRing.get(targetQIndex);
                            chordRing.remove(tmpQNode);
                            Collections.sort(chordRing);
                            recInp.setNode(targetQNode);
                            OutputStream respQMsgOpStrm = socket.getOutputStream();
                            ObjectOutputStream respQSendOp = new ObjectOutputStream(respQMsgOpStrm);
                            respQSendOp.writeObject(recInp);
                            respQSendOp.flush();
                            break;
                        case 8:
                            //String selectionKeyHash = recInp.getMsg().split(":")[0];
                            String selectionKey = recInp.getMsg().split(":")[1];
                            FileInputStream inputStream;
                            try {
                                inputStream = getContext().openFileInput(selectionKey);
                                BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
                                String line = reader.readLine();
                                recInp.setMsg(recInp.getMsg()+":"+line);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            OutputStream respQInsMsgOpStrm = socket.getOutputStream();
                            ObjectOutputStream respQInsSendOp = new ObjectOutputStream(respQInsMsgOpStrm);
                            respQInsSendOp.writeObject(recInp);
                            respQInsSendOp.flush();
                            break;
                        case 9:
                            int isDel = delete(mUri, "@", null);
                            break;
                        case 10:
                            String[] dSplit = recInp.getMsg().split(":");
                            Node dNode = new Node(recInp.getAvd(), recInp.getFromPort());
                            dNode.setHashValue(dSplit[0]);
                            chordRing.add(dNode);
                            Collections.sort(chordRing);
                            int dIndex;
                            if(chordRing.indexOf(dNode) == chordRing.size()-1){
                                dIndex = 0;
                            }
                            else{
                                dIndex = chordRing.indexOf(dNode) + 1;
                            }
                            Node dfNode = chordRing.get(dIndex);
                            chordRing.remove(dNode);
                            Collections.sort(chordRing);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, recInp.getMsg()+"#"+dfNode.toString(), myPort, myAvd, "DeleteAtNode", "uni");
                            break;
                        case 11:
                            //String dKeyHash = recInp.getMsg().split(":")[0];
                            String dKey = recInp.getMsg().split(":")[1];
                            Log.d("dcheck", "rmsg: "+recInp.getMsg());
                            Log.d("dcheck", "selection: "+dKey);
                            deleteFile(dKey);
                            break;
                        default:
                            break;
                    }
                    socket.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            return;
        }
    }

    //From PA1
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            int nDevices = 0;
            String msgValue = msgs[0];
            int currentPort = Integer.parseInt(msgs[1]);
            int currentAvd = Integer.parseInt(msgs[2]);
            String msgType = msgs[3];
            String multiType = msgs[4];
            Message msgObject = null;
            if(msgType == "GetAllFiles"){
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 1);
            }
            else if(msgType == "RespAllFiles"){}
            else if(msgType == "NodeJoinRequest"){
                Log.d("nodejoin", "sending request");
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 3);
                msgObject.setToPort(SUPER_NODE_PORT);
                msgObject.setNode(myNode);
            }
            else if(msgType == "NodeJoinResponse"){
                Log.d("nodejoin", "sending response");
                Node sNode = deSerializeNode(msgValue);
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 4);
                msgObject.setToPort(sNode.getPort());
                msgObject.setNode(sNode);
            }
            else if(msgType == "GetNodeLocationInsert"){
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 5);
                msgObject.setToPort(SUPER_NODE_PORT);
            }
            else if(msgType == "InsertAtNode"){
                String[] mSplit = msgValue.split("#");
                msgValue = mSplit[0];
                Node tNode = deSerializeNode(mSplit[1]);
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 6);
                msgObject.setToPort(tNode.getPort());
            }
            else if(msgType == "GetNodeLocationQuery"){}
            else if(msgType == "GetQueryAtNode"){}
            else if(msgType == "DeleteAllFiles"){
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 9);
            }
            else if(msgType == "FindAndDeleteAtNode"){
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 10);
                msgObject.setToPort(SUPER_NODE_PORT);
            }
            else if(msgType == "DeleteAtNode"){
                String[] mSplit = msgValue.split("#");
                msgValue = mSplit[0];
                Node tdNode = deSerializeNode(mSplit[1]);
                msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 11);
                msgObject.setToPort(tdNode.getPort());
            }
            if(multiType == "multi") {//Sends a message to all the nodes
                while (nDevices < 5) {
                    try {
                        int remotePort = allPorts[nDevices];
                        msgObject.setToPort(remotePort);
                        Socket multiSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
                        //https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
                        OutputStream msgOpStrm = multiSocket.getOutputStream();
                        ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
                        sendOp.writeObject(msgObject);
                        sendOp.flush();
                        multiSocket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException" + e);
                    }
                    nDevices += 1;
                }
            }
            else if(multiType == "uni"){//Sends a message to one particular node
                try {
                    Socket uniSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msgObject.getToPort());
                    OutputStream msgOpStrm = uniSocket.getOutputStream();
                    ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
                    sendOp.writeObject(msgObject);
                    sendOp.flush();
                    uniSocket.close();
                } catch (UnknownHostException e) {
                    Log.e("ServerError", "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e("ServerError", "ClientTask socket IOException"+e);
                }
            }
            return null;
        }
    }

    //The function is used to deserialize a Node data.
    //Serialization is done by overriding the toString() method by implementing it in the Node class
    public Node deSerializeNode(String node){
        String[] nSplit = node.split(":");
        Node dNode = new Node(Integer.parseInt(nSplit[1]), Integer.parseInt(nSplit[0]));
        dNode.setHashValue(nSplit[2]);
        if(nSplit[3] != "null")
            dNode.setPredecessor(nSplit[3]);
        if(nSplit[4] != "null")
            dNode.setSuccessor(nSplit[4]);
        return dNode;
    }

    //Use the genHash function to generate a hash value for the given key
    public String getKeyHash(String key){
        String keyHash = null;
        try {
            keyHash = genHash(key);
        }
        catch (NoSuchAlgorithmException nsae){
            Log.e("DeleteKeyHash", "delete: "+nsae);
        }
        return keyHash;
    }

    //http://www.lucazanini.eu/en/2016/android/saving-reading-files-internal-storage/
    public void saveFile(String key, String value){
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Delete the given file from the file storage using the file name (key)
    public void deleteFile(String fileName){
        try {
            getContext().deleteFile(fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Function to check if a particular key value's hash lies in the current AVD's partition
    public boolean isBelongToMyNode(String keyHash){
        List<String> tmpList = Arrays.asList(myNode.getPredecessor(), keyHash, myNode.getHashValue());
        Collections.sort(tmpList);
        //If the temp list after sorting doesn't change, then the key lies in the current AVD's partition
        if (tmpList.get(0) == myNode.getPredecessor() && tmpList.get(1) == keyHash && tmpList.get(2) == myNode.getHashValue()){
            return true;
        }
        return false;
    }
}

//Class to represent each AVD as a node in the chord ring
class Node implements Comparable<Node>, Serializable{
    private int port;
    private int avd;
    private String hashValue;
    private String successor;
    private String predecessor;

    public Node(int avd, int port) {
        this.port = port;
        this.avd = avd;
        try {
            this.hashValue = genHash(String.valueOf(avd));
        }
        catch (NoSuchAlgorithmException nsae){
            Log.e("NodeConstructorError", "Node: "+nsae);
        }
    }

    public String getHashValue(){
        return this.hashValue;
    }

    public int getPort(){
        return this.port;
    }

    public int getAvd(){
        return this.avd;
    }

    public String getSuccessor(){
        return this.successor;
    }

    public String getPredecessor(){
        return this.predecessor;
    }

    public void setHashValue(String hv){
        this.hashValue = hv;
    }

    public void setSuccessor(String succ){
        this.successor = succ;
    }

    public void setPredecessor(String pred){
        this.predecessor = pred;
    }

    public String toString(){
        return String.valueOf(this.port)+":"+String.valueOf(this.avd)+":"+this.getHashValue()+":"+String.valueOf(this.predecessor)+":"+String.valueOf(this.successor);
    }

    //Sort the Nodes w.r.t the hash value of the node ie., avd number
    @Override
    public int compareTo(Node n) {
        return this.getHashValue().compareTo(n.getHashValue());
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

//Class to send the message as an object during multicast and unicast
class Message implements Serializable {

    private String msg;
    private int fromPort;
    private int toPort;
    private int avd;
    private String type;
    private int typeCode;
    private List<String> valList;
    private Node node;

    public Message(String msg, int port, int avd, String type, int typeCode){
        this.msg = msg;
        this.fromPort = port;
        this.avd = avd;
        this.type = type;
        this.typeCode = typeCode;
    }

    public String getMsg(){
        return this.msg;
    }

    public int getFromPort(){
        return this.fromPort;
    }

    public int getToPort(){
        return this.toPort;
    }

    public int getAvd(){
        return this.avd;
    }

    public String getType(){
        return this.type;
    }

    public int getTypeCode(){
        return this.typeCode;
    }

    public List getValList(){ return this.valList; }

    public Node getNode(){
        return this.node;
    }

    public void setMsg(String msg){
        this.msg = msg;
    }

    public void setToPort(int port){ this.toPort = port; }

    public void setValList(List<String> values){
        this.valList = values;
    }

    public void setNode(Node n){
        this.node = n;
    }
}
