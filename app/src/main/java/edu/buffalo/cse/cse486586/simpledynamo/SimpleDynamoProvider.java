package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = "SimpleDynamoProvider";
	private SimpleDynamoDBHelper gDBHelper = null;
	private static final String DATABASE_NAME = "simpledynamo";
	private static final int DATABASE_VERSION = 1;
	private SQLiteDatabase gDB;
	private Uri mUri;

    ServerSocket serverSocket;

    private class Node{
        int portNo;
        String serialNum;
        String nodeID;
        int predecessor;
        int successor;
        int tail;
        char status;

        public Node(int portNo, String serialNum, int predecessor, int successor, int tail, char status) {
            this.portNo = portNo;
            this.serialNum = serialNum;
            this.nodeID = genHash(serialNum);
            this.predecessor = predecessor;
            this.successor = successor;
            this.tail = tail;
            this.status = status; //'L': Live and 'F': Fail
        }
    }

	static final int SERVER_PORT = 10000;

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	private int myPort; //stores the port number
    private List<Node> ring = new ArrayList<Node>();

    private boolean recovered = false;

//	private String mySerialNum; //Serial Number of current node
//	private String myNodeID; //stores the hashed value of the serial number
//	private int myPredecessor; //stores the predecessor of the node
//	private int mySuccessor; //stores the successor of the node
//	private int myTail; //stores the tail node, if this node is a coordinator

    /* Methods */

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	/*Function to initialize the ring*/
	private void initializeRing(){
	    ring.add(new Node(11108,"5554",11112,11116,11120,'L'));
        ring.add(new Node(11116,"5558",11108,11120,11124,'L'));
        ring.add(new Node(11120,"5560",11116,11124,11112,'L'));
        ring.add(new Node(11124,"5562",11120,11112,11108,'L'));
        ring.add(new Node(11112,"5556",11124,11108,11116,'L'));
    }

	@Override
	public boolean onCreate() {

        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        Log.d(TAG,tel.getLine1Number());
        String serialNum = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(serialNum) * 2;

        //Initialize chord ring
        initializeRing();

        //Check if DB already exists
        File db = this.getContext().getDatabasePath(DATABASE_NAME);
        if(db.exists()){
            //this means that node has recovered
            recovered = true;
        }

        //Initialize database
        gDBHelper = new SimpleDynamoDBHelper(getContext(), DATABASE_NAME, null, DATABASE_VERSION);

        if(recovered){

            String recoveryMsg = "R#"+myPort;
            Log.d(TAG,"I have recovered!!!!! "+myPort+ " & message to write: "+recoveryMsg);
            try {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,recoveryMsg).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        else{
            //just creating an empty database
            gDB = gDBHelper.getReadableDatabase();
            gDB.close();
        }

        //Initialize serversocket
        try{
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch(IOException ioe){
            Log.e(TAG, "Can't create a ServerSocket");
            ioe.printStackTrace();
        }

		return true;
	}

    /*
    A helper function which compares the key with the node ID to see whether the key belongs to this node
    It returns true if yes, else returns false
     */
    private boolean isCorrectNode(String key, String nodeID, int predecessor, int successor){
        String hash_key = genHash(key);
        String hash_predecessor = genHash(String.valueOf(predecessor/2));
        //if only one node in the chord ring, insert/query the values in it or insert the node after it
        if(predecessor == successor && predecessor == myPort){
            return true;
        }
        //if key/node greater than predecessor and predecessor greater than current node
        else if(hash_key.compareTo(hash_predecessor)>0 && hash_predecessor.compareTo(nodeID) > 0){
            return true;
        }
        //if key/node lesser than current node and predecessor greater than current node
        else if(hash_key.compareTo(nodeID)<0 && hash_predecessor.compareTo(nodeID) > 0){
            return true;
        }
        //if key/node lesser than or equal to current node and predecessor lesser than key, then insert/query in current node
        else if(hash_key.compareTo(nodeID) <= 0 && hash_key.compareTo(hash_predecessor) > 0){
            return true;
        }
        return false;
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        int deletedRows = 0;

        //key appended with origin serial number  (i.e. where the request was made)
        String[] para = selection.split(";");

        try {
            Cursor result = null;
            Log.v("delete", selection);
            //check if the key belongs to current node, then delete it
            Log.v(TAG,"To de deleted: "+selection);

            //delete from coordinator, successor and tail
            if (selection.contains(";")) {
                Log.v("delete", "In current Node: "+selection.toString());
                gDB = gDBHelper.getWritableDatabase();
                try {
                    String[] keys = {para[0]};
                    String selClause = "key = ?";
                    Log.d("Delete", selection);
                    deletedRows = gDB.delete("content", selClause, keys);
                } catch (SQLiteException sqle) {
                    Log.e(TAG, "delete failed : " + sqle.toString());
                }
                Log.v("delete done: ", selection.toString());
            }
            else {
                //loop through the list of nodes, check which node will have that data
                // and directly send delete message to that node, successor and tail
                for (Node node : ring) {
                    if (isCorrectNode(para[0], node.nodeID, node.predecessor, node.successor)) {
                        String serialNum = String.valueOf(myPort / 2);
                        Node succNode = null, tailNode = null;
                        //get successor and tail nodes
                        for(Node n:ring){
                            if(n.portNo == node.successor)
                                succNode = n;
                            if(n.portNo == node.tail)
                                tailNode = n;
                        }
                        //Send the delete message to coordinator
                        if(node.portNo == myPort){
                            Log.v("delete", "In current Node: "+selection.toString());
                            gDB = gDBHelper.getWritableDatabase();
                            try {
                                String[] keys = {para[0]};
                                String selClause = "key = ?";
                                Log.d("Delete", selection);
                                deletedRows = gDB.delete("content", selClause, keys);
                            } catch (SQLiteException sqle) {
                                Log.e(TAG, "delete failed : " + sqle.toString());
                            }
                            Log.v("delete done: ", selection.toString());
                        }
                        else{
                            Message deleteMsg = new DataMessage("D", serialNum, node.portNo, para[0] + ";" + serialNum, null);
                            Log.d(TAG, "Delete Message for coordinator: " + deleteMsg.toString());
                            String resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMsg.toString()).get();
                            if(resultString != null) {
                                String[] par = resultString.split("#");
                                deletedRows += Integer.parseInt(par[3]);
                            }
                        }
                        //Delete from successor
                        if(succNode.status=='L') {
                            Message deleteMsg = new DataMessage("D", serialNum, node.successor, para[0] + ";" + serialNum, null);
                            Log.d(TAG, "Delete Message for successor: " + deleteMsg.toString());
                            String resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMsg.toString()).get();
                            if (resultString != null) {
                                String[] par = resultString.split("#");
                                deletedRows += Integer.parseInt(par[3]);
                            }
                        }

                        //Delete from tail
                        if(tailNode.status=='L') {
                            Message deleteMsg = new DataMessage("D", serialNum, node.tail, para[0] + ";" + serialNum, null);
                            Log.d(TAG, "Delete Message for tail: " + deleteMsg.toString());
                            String resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleteMsg.toString()).get();
                            if (resultString != null) {
                                String[] par = resultString.split("#");
                                deletedRows += Integer.parseInt(par[3]);
                            }
                        }
                    }
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
//        finally {
//            gDB.close();
//        }

		return deletedRows;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        String key = (String) values.get(KEY_FIELD);
        String value = (String) values.get(VALUE_FIELD);

        //key appended with origin serial number  (i.e. where the request was made)
        String[] para = key.split(";");

        try {
            Cursor result = null;
            Log.v("insert", para[0]);

            //insert the key-value pair in the database
            if (key.contains(";")) {
                ContentValues values1 = new ContentValues();
                values1.put(KEY_FIELD,para[0]);
                values1.put(VALUE_FIELD,value);
                Log.v("insert", "In current Node: "+key.toString());
                gDB = gDBHelper.getWritableDatabase();
                try {
                    gDB.insertWithOnConflict("content", null, values1, SQLiteDatabase.CONFLICT_REPLACE);
                }
                catch(SQLiteException sqle){
                    Log.e(TAG, "insertWithOnConflict failed : "+sqle.toString());
                }
                Log.v("Insert", "Insert in this node" + values1.toString());
            } else {
                //loop through the list of nodes, check which node will have that data
                // and directly send insert message to that node, successor and tail
                for (Node node : ring) {
                    Node succNode = null, tailNode = null;
                    //get successor and tail nodes
                    for(Node n:ring){
                        if(n.portNo == node.successor)
                            succNode = n;
                        if(n.portNo == node.tail)
                            tailNode = n;
                    }

                    if (isCorrectNode(para[0], node.nodeID, node.predecessor, node.successor)) {
                        String serialNum = String.valueOf(myPort / 2);

                        //if the current node is the coordinator, insert the key in it
                        if(node.portNo == myPort){
                            Log.v("insert", "In current Node: "+key.toString());
                            gDB = gDBHelper.getWritableDatabase();
                            try {
                                gDB.insertWithOnConflict("content", null, values, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                            catch(SQLiteException sqle){
                                Log.e(TAG, "insertWithOnConflict failed : "+sqle.toString());
                            }
                        }
                        else{ //Send the insert message to coordinator if it is alive
                            if(node.status == 'L') {
                                Message insertMsg = new DataMessage("I", serialNum, node.portNo, para[0] + ";" + serialNum, value);
                                Log.d(TAG, "Insert Message for coordinator: " + insertMsg.toString() + " Port : " + node.portNo);
                                String resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMsg.toString()).get();
//                            String[] par = resultString.split("#");
                            }

                        }
                        //Insert in successor if the successor is alive
                        if(succNode.status == 'L') {
                            Message insertMsg = new DataMessage("I", serialNum, node.successor, para[0] + ";" + serialNum, value);
                            Log.d(TAG, "Insert Message for successor: " + insertMsg.toString());
                            String resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMsg.toString()).get();
//                        String[] par = resultString.split("#");
                        }

                        //Insert in tail if the tail node is alive
                        if(tailNode.status == 'L') {
                            Message insertMsg = new DataMessage("I", serialNum, node.tail, para[0] + ";" + serialNum, value);
                            Log.d(TAG, "Insert Message for tail: " + insertMsg.toString());
                            String resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insertMsg.toString()).get();
//                        par = resultString.split("#");
                        }

                        return uri;
                    }
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
//        finally {
//            gDB.close();
//        }

        return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        String[] columnNames = {KEY_FIELD,VALUE_FIELD};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);
        Cursor result = null;
        String[] para = selection.split(";");

        try{

            Log.v("query", selection);

            //if local dump, return all the keys stored with current node
            if(para[0].equals("@")){
                gDB = gDBHelper.getReadableDatabase();
                result = gDB.query("content",null,null,null,null,null,null);
                result.moveToFirst();
            }
            else if(para[0].equals("*")){ //get data from all the nodes

                //Add your own data first
                gDB = gDBHelper.getReadableDatabase();
                result = gDB.query("content",null,null,null,null,null,null);
                result.moveToFirst();

                if(result != null && result.getCount() >0){
                    result.moveToFirst();
                    do{
                        Object[] objValues = new Object[]{result.getString(0),result.getString(1)};
                        matrixCursor.addRow(objValues);
                        Log.v("Star","Added my own data: key:"+result.getString(0)+" value-"+result.getString(1));
                    }while(result.moveToNext());
                }

                //Get the data from all the nodes by calling their query "@"
                for (Node node : ring) {
                    if(node.status == 'L') {
                        String serialNum = String.valueOf(myPort / 2);
                        Message queryMsg = new DataMessage("Q", serialNum, node.portNo, "@;" + serialNum, null);
                        Log.d(TAG, "Query Message: " + queryMsg.toString());

                        String resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg.toString()).get();

                        //append the result obtained in matrixcursor
                        if (resultString != null) {
                            String[] par = resultString.split("#");

                            //If it is not a QEND message, means data exists and add it to MatrixCursor
                            if (!par[3].equals("QEND")) {
                                String[] keys = par[3].split(":");
                                String[] values = par[4].split(":");
                                for (int i = 0; i < keys.length; i++) {
                                    Object[] objValues = new Object[]{keys[i], values[i]};
                                    matrixCursor.addRow(objValues);
                                    Log.v("Star", "Added received data in cursor: key:" + keys[i] + " value-" + values[i]);
                                }
                            }
                        }
                    }
                }
                return matrixCursor;
            }
            else {//if only one key requested
                //query at tail
                if (selection.contains(";")) {
                    result = null;
                    String[] keys = {para[0]};
                    String selClause = "key = ?";
                    //Log.d(TAG, selClause);
                    gDB = gDBHelper.getReadableDatabase();
                    result = gDB.query("content", projection, selClause, keys, null, null, sortOrder);
                    result.moveToFirst();
                } else {
                    //loop through the list of nodes, check which node will have that data and directly send query message to its tail
                    //and get the data
                    for (Node node : ring) {
                        Node tailNode = null;
                        //get tail nodes
                        for(Node n:ring){
                            if(n.portNo == node.tail)
                                tailNode = n;
                        }
                        if (isCorrectNode(para[0], node.nodeID, node.predecessor, node.successor)) {
                            String serialNum = String.valueOf(myPort / 2);
                            Message queryMsg = null;
                            Log.d(TAG, "TAIL node: "+ tailNode.portNo + " Status: "+tailNode.status);
                            String resultString = null;
                            if(tailNode.status == 'L') {
                                queryMsg = new DataMessage("Q", serialNum, node.tail, para[0] + ";" + serialNum, null);
                                Log.d(TAG, "Query Message to tail: " + queryMsg.toString());
                                resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg.toString()).get();
                            }
                            if(resultString == null || tailNode.status == 'F'){
                                queryMsg = new DataMessage("Q", serialNum, node.successor, para[0] + ";" + serialNum, null);
                                Log.d(TAG, "Query Message to successor: " + queryMsg.toString());
                                resultString = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, queryMsg.toString()).get();
                            }

                            if(resultString != null) {
                                String[] par = resultString.split("#");

                                //If it is a QEND message, return null
                                if (par[3].equals("QEND"))
                                    return null;
                                //Add the key value in a MatrixCursor and return it
                                Log.d(TAG, "Key-value pairs: " + par[3] + ": " + par[4]);
                                Object[] objValues = new Object[]{par[3], par[4]};
                                matrixCursor.addRow(objValues);
                                return matrixCursor;
                            }
                        }
                    }
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
//        finally {
//            gDB.close();
//        }
        return result;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input){
        Formatter formatter = new Formatter();
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
        }
        catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.d(TAG,"Server socket created!");

            while(true) {
//                Log.d(TAG,"Inside server while");
                try {
                    Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String str = in.readLine();
                    Log.d(TAG,"Message read on server side: "+str);

                    if(str != null){

                        String[] msgParameters = str.split("#");

                        if(str.startsWith("I")){ //If the message is insert
                            ContentValues cv = new ContentValues();
                            cv.put(KEY_FIELD, msgParameters[3]);
                            cv.put(VALUE_FIELD, msgParameters[4]);
                            Uri uri = insert(mUri, cv);
                            if(uri != null){
                                String result = "";
                                Message insertAck = new DataMessage("I", msgParameters[1],
                                        Integer.parseInt(msgParameters[1]) * 2, "Success",null);
                                result = insertAck.toString();
                                PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                                printWriter.println(result);
                                printWriter.flush();
                            }
                        }

                        else if(str.startsWith("Q")) { //If the message is query request

                            //It means that this is a tail node, query it and write the data back to the socket
                            Cursor resultCursor = query(mUri, null, msgParameters[3], null, null);
                            String result = null;

                            //If data exists, write it on the same socket
                            if (resultCursor != null && resultCursor.moveToFirst()) {
                                Message queryReplyMsg = new DataMessage("Q", msgParameters[1], Integer.parseInt(msgParameters[1]) * 2, resultCursor);
                                Log.v("Query","Server replies- "+queryReplyMsg.toString());
                                result = queryReplyMsg.toString();
                            }
                            else { //if data does not exist, just write "QEND"
                                Message queryReplyMsg = new DataMessage("Q", msgParameters[1], Integer.parseInt(msgParameters[1]) * 2, "QEND", null);
                                Log.v("Query","Server replies: if null- "+queryReplyMsg.toString());
                                result = queryReplyMsg.toString();
                            }
                            PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                            printWriter.println(result);
                            printWriter.flush();
                        }
                        else if(str.startsWith("D")){ //If the message is delete request
                            Log.v("Delete","Inside server, msg: "+str);
                            String result = "";
                            int deletedRows = delete(mUri, msgParameters[3], null);
                            Message deleteAck = new DataMessage("D", msgParameters[1],
                                    Integer.parseInt(msgParameters[1]) * 2, String.valueOf(deletedRows),null);
                            Log.v("Delete","Server replies- "+deleteAck.toString());
                            result = deleteAck.toString();

                            PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                            printWriter.println(result);
                            printWriter.flush();
                        }
                        else if(str.startsWith("C")){ //If the message is a crash notification
                            int crashedIndex = Integer.parseInt(msgParameters[1]);
                            ring.get(crashedIndex).status = 'F';
                            Log.d(TAG,"Crash node: "+ring.get(crashedIndex).serialNum
                                    +" : " +ring.get(crashedIndex).status);
                        }
                        else if(str.startsWith("R")){
                            Node crashedNode = null;
                            for(Node node: ring){
                                if(Integer.parseInt(msgParameters[1]) == node.portNo){
                                    node.status = 'L';
                                    crashedNode = node;
                                    break;
                                }
                            }
                            Log.d(TAG,"Recovered node: "+crashedNode.serialNum+" : "+crashedNode.status);
                            //Query your own database and get all local data

                            String result = null;
                            if(msgParameters[2].equals("ReplicaData")){
                                Node currentNode = null;
                                for(Node n: ring){
                                    if(n.portNo == myPort){
                                        currentNode = n;
                                        break;
                                    }
                                }
                                result = recoverData(currentNode,msgParameters[1]);
                            }
                            else if(msgParameters[2].equals("MyData"))
                                result = recoverData(crashedNode,msgParameters[1]);
                            else{ //If no data needed from this node
                                Message recoveryReplyMsg = new DataMessage("R", msgParameters[1], Integer.parseInt(msgParameters[1]) * 2, "QEND", null);
                                Log.v("Recovery","Need no data: Server replies: if null- "+recoveryReplyMsg.toString());
                                result = recoveryReplyMsg.toString();
                            }

                            PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                            printWriter.println(result);
                            printWriter.flush();
                        }
                    }
                } catch (IOException ioe) {
                    Log.e(TAG, "ServerTask socket IOException");
                    ioe.printStackTrace();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }

        private String recoverData(Node node, String recoveredPort){
            Cursor resultCursor = query(mUri, null, "@", null, null);
            String[] columnNames = {KEY_FIELD,VALUE_FIELD};
            MatrixCursor matrixCursor = new MatrixCursor(columnNames);
            String result = null;
            if(resultCursor != null && resultCursor.getCount() >0){
                resultCursor.moveToFirst();
                do{
                    if(isCorrectNode(resultCursor.getString(0),node.nodeID,
                            node.predecessor,node.successor)) {
                        Object[] objValues = new Object[]{resultCursor.getString(0), resultCursor.getString(1)};
                        matrixCursor.addRow(objValues);
                    }
                }while(resultCursor.moveToNext());

                if(matrixCursor != null && matrixCursor.getCount() > 0) {
                    Message recoveryReplyMsg = new DataMessage("R", recoveredPort, Integer.parseInt(recoveredPort), matrixCursor);
                    Log.v("Recovery", "Server replies- " + recoveryReplyMsg.toString());
                    result = recoveryReplyMsg.toString();
                }
                else { //if data does not exist, just write "QEND"
                    Message recoveryReplyMsg = new DataMessage("R", recoveredPort, Integer.parseInt(recoveredPort), "QEND", null);
                    Log.v("Recovery","Server replies: if null- "+recoveryReplyMsg.toString());
                    result = recoveryReplyMsg.toString();
                }
            }
            else { //if data does not exist, just write "QEND"
                Message recoveryReplyMsg = new DataMessage("R", recoveredPort, Integer.parseInt(recoveredPort), "QEND", null);
                Log.v("Recovery","Server replies: if null- "+recoveryReplyMsg.toString());
                result = recoveryReplyMsg.toString();
            }
            return result;
        }

        protected void onProgressUpdate(String...strings) {
            //Invoke the client AsyncTask in publishProgress as cannot be invoked from doInBackground
            String msg = strings[0];
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {
        @Override
        protected String doInBackground(String... msgs) {
            String msg = msgs[0];
            String[] parameters = msg.split("#");
            String result = null;

            try {
                if(parameters[0].equals("R")){
                    //if a node has recovered, then notify all the nodes and get data from predecessor, predecessor's predecessor
                    //and get your own data from successor
                    int pred = 0, succ = 0, pred_pred = 0;
                    for(Node n: ring){
                        if(n.portNo == myPort){
                            pred = n.predecessor;
                            succ = n.successor;
                            break;
                        }
                    }
                    for(Node n:ring){
                        if(pred == n.portNo){
                            pred_pred = n.predecessor;
                            break;
                        }
                    }

                    for(Node node: ring){
                        //write recovery message to all the ports except the current one
                        if(node.portNo != myPort) {
                            Socket socket = new Socket();
                            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node.portNo));

                            String recoveryMsg = null;
                            if(node.portNo == pred || node.portNo == pred_pred)
                                recoveryMsg = msg + "#ReplicaData";
                            else if(node.portNo == succ)
                                recoveryMsg = msg + "#MyData";
                            else
                                recoveryMsg = msg + "#NoData";

                            Log.d(TAG,"Recovery Message sent to nodes: "+recoveryMsg + " port no: "+node.serialNum);
                            PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                            printWriter.println(recoveryMsg);
                            printWriter.flush();

                            //Read the data from the nodes and insert all the data in DB
                            InputStreamReader input = new InputStreamReader(socket.getInputStream());
                            BufferedReader in = new BufferedReader(input);
                            result = in.readLine();

                            String[] resString = result.split("#");
                            if(!resString[3].equals("QEND")){
                                String[] keys = resString[3].split(":");
                                String[] vals = resString[4].split(":");
                                for(int i=0;i<keys.length;i++){
                                    ContentValues cv = new ContentValues();
                                    cv.put(KEY_FIELD, keys[i]+";");
                                    cv.put(VALUE_FIELD, vals[i]);
                                    Log.d(TAG, "Recovery Insert on my node: " + keys[i] + " from Port: "+ node.portNo);
                                    insert(mUri, cv);
                                }
                            }
                        }
                    }
                }
                else {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(parameters[2])));

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(msg);
                    printWriter.flush();
                    //Log.v("Star","Client: Write message: "+msg);

                    //  if(parameters[0].equals("Q")){
                    InputStreamReader input = new InputStreamReader(socket.getInputStream());
                    BufferedReader in = new BufferedReader(input);
                    result = in.readLine();
                    Log.v(TAG, "Client: Result: " + result);
//                    if(result != null)
//                        return result;
                    //   }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException on Message read: " + e.getMessage());
                e.printStackTrace();
                handleFailure(Integer.parseInt(parameters[2]));
                return null;
            }

            return result;
        }

        /*
        Function to handle the AVD crash
        */
        public void handleFailure(int crashedPort) {
            //notify every node in the ring that the avd has crashed
            int crashedNodeIndex=-1;

            for(Node node: ring){
                if(node.portNo == crashedPort) {
                    //get index of the crashed node in the ring list
                    crashedNodeIndex = ring.indexOf(node);
                    ring.get(crashedNodeIndex).status = 'F';
                    Log.d(TAG,"I detected the node crash: "+ring.get(crashedNodeIndex).serialNum
                            +" " +ring.get(crashedNodeIndex).status);
                    break;
                }
            }
            Log.d(TAG,"Crashed node index: "+ crashedNodeIndex + " and node is " + ring.get(crashedNodeIndex).serialNum);

            for(Node node: ring){
                if(node.portNo != myPort && ring.indexOf(node)!=crashedNodeIndex){
                    try{
                        Log.d(TAG, "Contacting node: "+node.portNo);
                        Socket socket = new Socket();
                        socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node.portNo));

                        String msg = "C#"+crashedNodeIndex;
                        PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                        printWriter.println(msg);
                        printWriter.flush();
                    }
                    catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    }
                    catch (IOException e) {
                        Log.e(TAG, "Socket exception on notification: "+node.portNo);
                    }
                }
            }
        }
    }
}
