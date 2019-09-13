package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String chordLowestValue = "0000000000000000000000000000000000000000";
    static final String chordHighestValue = "ffffffffffffffffffffffffffffffffffffffff";
    private static String myPort = "";
    private static String myPortHash = "";
    private static final String masterPort = "5554";
    private static String successorNodeHash = "";
    private static String predecessorNodeHash = "";
    private static String successorNodePortNumber = "";
    private static String predecessorNodePortNumber = "";
    private ContentResolver mContentResolver = null;
    private Uri mUri = null;
    private BlockingQueue<String> messageDeliveryQueue = new LinkedBlockingQueue<String>(1);

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    public SimpleDhtProvider() {
        try {
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        }
        catch (Exception e) {
            Log.e(TAG, "Can't create a ServerSocket" + e.getMessage());
            return;
        }
    }

    public SimpleDhtProvider(String port, boolean joinChord, ContentResolver _cr){

        try {
            Log.e(TAG, "Hi" );
            myPort = port;
            myPortHash = genHash(port);
            mContentResolver = _cr;
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            mContentResolver.delete(mUri, "@",null);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket" + e.getMessage());
            return;
        }
        catch (Exception e) {
            Log.e(TAG, "Can't create a ServerSocket" + e.getMessage());
            return;
        }

        if(joinChord){
            RequestToJoinIntoChord(port,masterPort);
        }

    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void RequestToJoinIntoChord(String port, String portToRequest){
        String requestType = "JoinIntoChord";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestType, port, portToRequest);
    }

    private void NotifyJoinChordComplete(String portToNotify, String successor , String predecessor){
        String requestType = "JoinChordComplete";
        String messagesToMove = GetMessagesToMove(predecessor,portToNotify);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestType, myPort, portToNotify, successor, predecessor, messagesToMove);
    }

    private String GetMessagesToMove(String fromNode, String toNode){
        String result = "";

        try {
            Cursor cursor = mContentResolver.query(mUri, null, "range", new String[]{fromNode, toNode}, null);

            RemoveMessagesFromAvd(cursor);

            result = GetFormatedMessageFromCursor(cursor);
        }
        catch (Exception ex){
            Log.e("GetMessagesToMove", "Message: " + ex.getMessage());
        }

        return result;
    }

    private String GetFormatedMessageFromCursor(Cursor cursor){
        StringBuilder result = new StringBuilder();

        int keyIndex = cursor.getColumnIndex(KEY_FIELD);
        int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

        if (cursor != null) {

            cursor.moveToFirst();
            for (int i = 0; i < cursor.getCount(); i++) {
                if(i != 0){
                    result.append("---");
                }
                String key = cursor.getString(keyIndex);
                String value = cursor.getString(valueIndex);

                result.append(key + "#" + value);

                cursor.moveToNext();
            }
            cursor.close();
        }

        return result.toString();

        /*
        https://stackoverflow.com/questions/10081631/android-cursor-movetonext
        */
    }

    private void RemoveMessagesFromAvd(Cursor cursor){
        int keyIndex = cursor.getColumnIndex(KEY_FIELD);

        if (cursor != null) {

            cursor.moveToFirst();
            for (int i = 0; i < cursor.getCount(); i++) {

                String key = cursor.getString(keyIndex);
                mContentResolver.delete(mUri,key,null);
                cursor.moveToNext();
            }
            cursor.close();
        }
    }


    private void NotifyChordToUpdateSuccessor(String portToNotify, String successor){
        String requestType = "UpdateSuccessor";
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestType, myPort, portToNotify, successor);
    }

    private void ForwardInsertRequestToSuccessor(ContentValues values){
        String requestType = "InsertRequest";
        String portToForward = successorNodePortNumber;
        if(predecessorNodePortNumber == "" && successorNodePortNumber == "")
        {
            return;
        }
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String message = key + "#" + value;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestType, myPort, portToForward, message);
    }

    private void ForwardDeleteAllRequestToSuccessor(String originator){
        String requestType = "DeleteAllRequest";
        String portToForward = successorNodePortNumber;
        if(predecessorNodePortNumber == "" && successorNodePortNumber == "")
        {
            return;
        }
        if(!originator.trim().equals(portToForward.trim()) )
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestType, myPort, portToForward, originator);
    }


    private String ForwardGetAllRequestToSuccessor(String originator){
        String result = "None";
        try{
            String requestType = "GetAllRequest";
            String portToForward = successorNodePortNumber;
            if(predecessorNodePortNumber == "" && successorNodePortNumber == "")
            {
                return null;
            }
            if(!originator.trim().equals(portToForward.trim())) {

                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,requestType, myPort, portToForward, originator);
                //Log.e(TAG, "Client AA gaya data = " + messageDeliveryQueue.take() );
                result = messageDeliveryQueue.take();
            }
        }
        catch (Exception ex){
        }
        return result;

    }

/*
    private String ForwardGetAllRequestToSuccessor(String originator){
        String result = "None";
        try{
            String requestType = "GetAllRequest";
            String portToForward = successorNodePortNumber;
            if(predecessorNodePortNumber == "" && successorNodePortNumber == "")
            {
                return null;
            }
            if(!originator.trim().equals(portToForward.trim())) {

                result = SendRequest(requestType, myPort, portToForward, originator);
            }
        }
        catch (Exception ex){
        }
        return result;

    }
*/
    private MatrixCursor ForwardGetRequestToSuccessor(String originator, String selection){

        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );
        try{
            String requestType = "GetRequest";
            String portToForward = successorNodePortNumber;
            if(predecessorNodePortNumber == "" && successorNodePortNumber == "")
            {
                return null;
            }
            if(!originator.trim().equals(portToForward.trim())) {

                String result = SendRequest(requestType, myPort, portToForward, originator,selection);
                if(!result.equals("None"))
                    cursor = GetCursorFromMessages(result, cursor);
            }
        }
        catch (Exception ex){

        }

        return cursor;
    }

    private String SendRequest(String... arguments){
        String requestType = arguments[0];
        String myPort = arguments[1];
        String portToConnect = String.valueOf((Integer.parseInt(arguments[2]) * 2));
        int timeout = 2000;
        String getRequestResult = "None";

        String msgToSend =  GetFormattedMessage(requestType, myPort,arguments);
        Log.e(TAG, "Client Started = " + msgToSend );
        if(msgToSend == ""){
            return getRequestResult;
        }

        Socket socket = new Socket();
        try{

            SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToConnect));

            socket.connect(socketAddress, timeout);

            PrintWriter outputPrintWriter = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader inputBufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            outputPrintWriter.println(msgToSend);
            String messageFromServer = "";
            while ((messageFromServer = inputBufferedReader.readLine()) != null) {
                if(messageFromServer.contains("GetRequestResult")){
                    getRequestResult = messageFromServer.substring(17);
                }
                if(messageFromServer.contains("GetRequestAllResult")){
                    getRequestResult = messageFromServer.substring(17);
                }
                if (messageFromServer.contains("MESSAGE-RECEIVED")) {
                    break;
                }
            }
            inputBufferedReader.close();
            outputPrintWriter.close();
            socket.close();
            if (requestType.equals("GetRequest")){
                return getRequestResult;
            }
            if (requestType.equals("GetAllRequest")){
                return getRequestResult;
            }

        }catch (SocketTimeoutException e) {
            Log.e(TAG, "ClientTask Socket timeout: " + portToConnect);
        }catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException: " + portToConnect);
        }catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException 1: " + e.getMessage() + "  " + e.getStackTrace() + " Port:  " + portToConnect);
        }catch (Exception ex){
            Log.e(TAG, "ClientTask Exception: " + ex.getMessage() + ": Port: " + portToConnect);
        }finally {
            try {
                socket.close();
            }catch (Exception ex){
                Log.e(TAG, "ClientTask Exception: " + ex.getMessage() + ": Port: " + portToConnect);
            }
        }

        return getRequestResult;
    }



    private void SendDataToOriginator(MatrixCursor cursor, String originator){
        String requestType = "SendGetAllData";
        String portToSend = originator;
        String message = GetMessageFromCursor(cursor);
        String completed = "no";
        if(successorNodePortNumber.equals(originator)){
            completed = "yes";
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestType, myPort, portToSend, message, completed);
    }

    private String GetMessageFromCursor(MatrixCursor cursor){
        StringBuilder result = new StringBuilder();

        int keyIndex = cursor.getColumnIndex(KEY_FIELD);
        int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

        if (cursor != null) {

            cursor.moveToFirst();
            for (int i = 0; i < cursor.getCount(); i++) {
                if(i != 0){
                    result.append("---");
                }
                String key = cursor.getString(keyIndex);
                String value = cursor.getString(valueIndex);

                result.append(key + "#" + value);

                cursor.moveToNext();
            }
            cursor.close();
        }

        return result.toString();

        /*
        https://stackoverflow.com/questions/10081631/android-cursor-movetonext
        */
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("@")){
            DeleteAllFileFromInternalStorage();
        }
        else if(selection.equals("*")){
            DeleteAllFileFromInternalStorage();
            if(selectionArgs == null){
                ForwardDeleteAllRequestToSuccessor(myPort);
            }
            else {
                ForwardDeleteAllRequestToSuccessor(selectionArgs[0]);
            }

        }
        else {
            DeleteFileFromInternalStorage(selection);
        }
        return 0;
    }

    private void DeleteFileFromInternalStorage(String filename){

        Context appContext = getContext();
        try {
            appContext.deleteFile(filename);
        } catch (Exception e) {
            Log.e("Delete File", "File delete failed");
        }
    }

    private void DeleteAllFileFromInternalStorage(){

        Context appContext = getContext();
        try {
            String[] listOfFiles = appContext.fileList();
            for(String name: listOfFiles){
                appContext.deleteFile(name);
            }
        } catch (Exception e) {
            Log.e("Delete File", "File delete failed");
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.v("insert", values.toString());
        try{
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String keyHash = genHash(key);

            if(isNodeBehind(keyHash)){
                //Log.e(TAG, "inserting = Key:" + key + "  Value:" + value);
                WriteMessageIntoInternalStorage(key,value + "\n");
            }
            else{
                ForwardInsertRequestToSuccessor(values);
            }
        }
        catch (Exception ex){

        }
        return uri;
    }

    private void WriteMessageIntoInternalStorage(String filename, String content){

        FileOutputStream outputStream;
        Context appContext = getContext();

        try {
            outputStream =  appContext.openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(content.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e("WriteMessage", "File write failed");
        }

        /*
        References:
        Usage has been referred from PA1.
        */
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        MatrixCursor cursor = null;
        if(selection.equals("@")){
            cursor = ReadAllMessagesFromInternalStorage();
        }
        else if(selection.equals("*")){
            if(selectionArgs == null){
                //ForwardGetAllRequestToSuccessor(myPort);
                //cursor = ReadAllMessagesFromInternalStorage();
                cursor = ReadMessagesFromAllAvds(myPort);
            }
            else {
                //ForwardGetAllRequestToSuccessor(selectionArgs[0]);
                //cursor = ReadAllMessagesFromInternalStorage();
                //SendDataToOriginator(cursor,selectionArgs[0]);
                cursor = ReadMessagesFromAllAvds(selectionArgs[0]);
            }
        }
        else if(selection.equals("range")){
            cursor = ReadRangeOfMessagesFromInternalStorage(selectionArgs[0], selectionArgs[1]);
        }
        else {
            cursor = ReadMessageFromInternalStorage(selection);
            if(cursor.getCount() < 1){
                if(selectionArgs == null){
                    cursor = ForwardGetRequestToSuccessor(myPort, selection);
                }
                else {
                    cursor = ForwardGetRequestToSuccessor(selectionArgs[0], selection);
                }

            }
        }
        /*
        References:
        https://stackoverflow.com/questions/18290864/create-a-cursor-from-hardcoded-array-instead-of-db
        */
        return cursor;
    }

    private MatrixCursor ReadMessagesFromAllAvds(String originator){

        MatrixCursor cursor = null;

        try {
            cursor = ReadAllMessagesFromInternalStorage();
            if(predecessorNodePortNumber == "" && successorNodePortNumber == "")
            {
                return cursor;
            }

            String messagesFromSuccessor = ForwardGetAllRequestToSuccessor(originator);
            if(messagesFromSuccessor != null && !messagesFromSuccessor.equals("None")){
                cursor = GetCursorFromMessages(messagesFromSuccessor,cursor);
            }

        } catch (Exception e) {
            Log.e("ReadMessagesFromAllAvds", "File read failed");
        }

        return cursor;
    }

    private MatrixCursor ReadRangeOfMessagesFromInternalStorage(String fromNode, String toNode){

        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );
        FileInputStream inputStream;
        String value = "";
        Context appContext = getContext();

        try {

            String fromNodeHash = genHash(fromNode);
            String toNodeHash = genHash(toNode);
            String[] listOfFiles = appContext.fileList();
            for(String key: listOfFiles){
                String keyHash = genHash(key);
                boolean result = false;
                if (fromNodeHash.compareTo(toNodeHash) < 0){
                    if(fromNodeHash.compareTo(keyHash) < 0 && toNodeHash.compareTo(keyHash) >= 0){
                        result = true;
                    }
                }
                else {
                    if((fromNodeHash.compareTo(keyHash) < 0 && chordHighestValue.compareTo(keyHash) >= 0) ||
                            (chordLowestValue.compareTo(keyHash) < 0 && toNodeHash.compareTo(keyHash) >= 0)){
                        result = true;
                    }
                }
                if(result){
                    inputStream =  appContext.openFileInput(key);
                    InputStreamReader streamReader = new InputStreamReader(inputStream);
                    BufferedReader bufferedReader = new BufferedReader(streamReader);
                    value = bufferedReader.readLine();
                    cursor.newRow()
                            .add("key", key)
                            .add("value", value);
                    inputStream.close();
                    //Log.e("ReadMessage", "Key: " + key +" value: " + value);
                }

            }

        } catch (Exception e) {
            Log.e("ReadMessage", "File read failed");
        }
        /*
        References:
        https://stackoverflow.com/questions/9095610/android-fileinputstream-read-txt-file-to-string
        */

        return cursor;
    }

    private MatrixCursor ReadMessageFromInternalStorage(String key){

        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );

        FileInputStream inputStream;
        String value = "";
        Context appContext = getContext();

        try {
            inputStream =  appContext.openFileInput(key);
            InputStreamReader streamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(streamReader);
            value = bufferedReader.readLine();
            inputStream.close();
            cursor.newRow()
                    .add("key", key)
                    .add("value", value);

            //Log.e("ReadMessage", "Key: " + key +" value: " + value);
        } catch (Exception e) {
            Log.e("ReadMessage", "File read failed");
        }


        /*
        References:
        https://stackoverflow.com/questions/9095610/android-fileinputstream-read-txt-file-to-string
        */

        return cursor;
    }

    private MatrixCursor ReadAllMessagesFromInternalStorage(){

        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );
        FileInputStream inputStream;
        String value = "";
        Context appContext = getContext();

        try {
            String[] listOfFiles = appContext.fileList();
            for(String key: listOfFiles){
                inputStream =  appContext.openFileInput(key);
                InputStreamReader streamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(streamReader);
                value = bufferedReader.readLine();
                cursor.newRow()
                        .add("key", key)
                        .add("value", value);
                inputStream.close();
                Log.e("ReadMessage", "Key: " + key +" value: " + value);
            }

        } catch (Exception e) {
            Log.e("ReadMessage", "File read failed");
        }
        /*
        References:
        https://stackoverflow.com/questions/9095610/android-fileinputstream-read-txt-file-to-string
        */

        return cursor;
    }

    private MatrixCursor GetCursorFromMessages(String messages, MatrixCursor cursor){
        String[] list = messages.trim().split("---",0);

        for(String message: list){
            String[] arrOfStr = message.split("#",0);
            String key = arrOfStr[0];
            String value = arrOfStr[1];
            cursor.newRow()
                    .add("key", key)
                    .add("value", value);
        }
        return cursor;
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

    private boolean isNodeBehind(String nodeHash){

        if(predecessorNodePortNumber== "" && successorNodePortNumber == ""){
            return true;
        }

        if (predecessorNodeHash.compareTo(myPortHash) < 0){
            if(predecessorNodeHash.compareTo(nodeHash) < 0 && myPortHash.compareTo(nodeHash) >= 0){
                return true;
            }
        }
        else {
            if((predecessorNodeHash.compareTo(nodeHash) < 0 && chordHighestValue.compareTo(nodeHash) >= 0) ||
                    (chordLowestValue.compareTo(nodeHash) < 0 && myPortHash.compareTo(nodeHash) >= 0)){
                return true;
            }
        }
        return false;
    }

    private String GetFormattedMessage(String requestType, String myPort, String... arguments){
        String formattedMessage = "";

        if(requestType.equals("JoinIntoChord")){
            formattedMessage="RequestType=JoinIntoChord&"+"MyPort=" + myPort;
        }
        else if(requestType.equals("JoinChordComplete")){
            formattedMessage="RequestType=JoinChordComplete&"+"MyPort=" + myPort + "&SuccessorNodePortNumber=" + arguments[3] + "&PredecessorNodePort=" + arguments[4] + "&Message=" + arguments[5];
        }
        else if(requestType.equals("InsertRequest")){
            formattedMessage="RequestType=InsertRequest&"+"MyPort=" + myPort + "&Message=" + arguments[3];
        }
        else if(requestType.equals("DeleteAllRequest")){
            formattedMessage="RequestType=DeleteAllRequest&"+"MyPort=" + myPort + "&Originator=" + arguments[3];
        }
        else if(requestType.equals("GetAllRequest")){
            formattedMessage="RequestType=GetAllRequest&"+"MyPort=" + myPort + "&Originator=" + arguments[3];
        }
        else if(requestType.equals("SendGetAllData")){
            formattedMessage="RequestType=SendGetAllData&"+"MyPort=" + myPort + "&Message=" + arguments[3] + "&Completed=" + arguments[4];
        }
        else if(requestType.equals("UpdateSuccessor")){
            formattedMessage="RequestType=UpdateSuccessor&"+"MyPort=" + myPort + "&SuccessorNodePortNumber=" + arguments[3];
        }
        else if(requestType.equals("GetRequest")){
            formattedMessage="RequestType=GetRequest&"+"MyPort=" + myPort + "&Originator=" + arguments[3] + "&Selection=" + arguments[4];
        }

        return formattedMessage;
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {

        protected Void doInBackground(String... arguments) {

            String requestType = arguments[0];
            String myPort = arguments[1];
            String portToConnect = String.valueOf((Integer.parseInt(arguments[2]) * 2));
            int timeout = 2000;
            String getRequestResult = "None";

            String msgToSend =  GetFormattedMessage(requestType, myPort,arguments);
            Log.e(TAG, "Client Started = " + msgToSend );
            if(msgToSend == ""){
                return null;
            }

            Socket socket = new Socket();
            try{

                SocketAddress socketAddress = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(portToConnect));

                socket.connect(socketAddress, timeout);

                PrintWriter outputPrintWriter = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader inputBufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                outputPrintWriter.println(msgToSend);
                String messageFromServer = "";
                while ((messageFromServer = inputBufferedReader.readLine()) != null) {
                    Log.e(TAG, "ClientTask messageFromServer: " + messageFromServer);
                    if(messageFromServer.contains("GetRequestAllResult")){
                        getRequestResult = messageFromServer.substring(20);
                    }
                    if (messageFromServer.contains("MESSAGE-RECEIVED")) {
                        break;
                    }
                }
                inputBufferedReader.close();
                outputPrintWriter.close();
                socket.close();
                if (requestType.equals("GetAllRequest")){
                    messageDeliveryQueue.put(getRequestResult);
                }

            }catch (SocketTimeoutException e) {
                Log.e(TAG, "ClientTask Socket timeout: " + portToConnect);
            }catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException: " + portToConnect);
            }catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException 1: " + e.getMessage() + "  " + e.getStackTrace() + " Port:  " + portToConnect);
            }catch (Exception ex){
                Log.e(TAG, "ClientTask Exception: " + ex.getMessage() + ": Port: " + portToConnect);
            }finally {
                try {
                    socket.close();
                }catch (Exception ex){
                    Log.e(TAG, "ClientTask Exception: " + ex.getMessage() + ": Port: " + portToConnect);
                }
            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        protected Void doInBackground(ServerSocket... sockets) {


            ServerSocket serverSocket = sockets[0];

            try{
                while(true){
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(500);
                    try{

                        PrintWriter outputPrintWriter = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader inputBufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String messageFromClient = "";

                        while ((messageFromClient = inputBufferedReader.readLine()) != null) {

                            if(messageFromClient.contains("GetRequest")){
                                String strReceived = messageFromClient.trim();
                                String[] arrOfStr = strReceived.split("&",0);
                                String originator = arrOfStr[2].substring(11);
                                String selection = arrOfStr[3].substring(10);
                                String reply = "GetRequestResult=" + StartProcessToGet(originator, selection);
                                outputPrintWriter.println(reply);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("GetAllRequest")){
                                String strReceived = messageFromClient.trim();
                                String[] arrOfStr = strReceived.split("&",0);
                                String originator = arrOfStr[2].substring(11);
                                Log.e(TAG, "ServerTask : hahahha");
                                String reply = "GetRequestAllResult=" + StartProcessToGetAll(originator);
                                Log.e(TAG, "ServerTask : hahahha " + reply);
                                outputPrintWriter.println(reply);
                                outputPrintWriter.println("MESSAGE-RECEIVED");

                                break;
                            }
                            else {
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }

                            /*
                            if (messageFromClient.contains("JoinIntoChord")){
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("JoinChordComplete")){
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("InsertRequest")){
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("DeleteAllRequest")){
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("GetAllRequest")){
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            else if(messageFromClient.contains("SendGetAllData")){
                                publishProgress(messageFromClient);
                                outputPrintWriter.println("MESSAGE-RECEIVED");
                                break;
                            }
                            */

                        }
                        inputBufferedReader.close();
                        outputPrintWriter.close();
                        socket.close();

                    }catch (SocketTimeoutException e) {
                        Log.e(TAG, "ServerTask Socket timeout: ");
                    }catch (IOException e) {
                        Log.e(TAG, "ServerTask IOException : " + e.getMessage() + "  " + e.getStackTrace());
                    }catch (Exception ex){
                        Log.e(TAG, "ServerTask Exception: " + ex.getMessage());
                    }finally {
                        socket.close();
                    }

                }

            } catch (Exception ex){
                Log.e(TAG, "ServerTask Exception: " + ex.getMessage());
            }
            finally{
                try{
                    serverSocket.close();
                } catch (Exception ex){
                    Log.e(TAG, "ServerTask Exception: " + ex.getMessage());
                }
            }

            return null;
        }

        protected synchronized void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
            String[] arrOfStr = strReceived.split("&",0);

            Log.e(TAG, "onProgressUpdate Message from client = " + strReceived );

            if(strReceived.contains("JoinIntoChord")){
                String port = arrOfStr[1].substring(7);
                Log.e(TAG, "onProgressUpdate Message from client = " + strReceived + " Port=" + port);
                StartProcessToJoinChord(port);
            }
            else if(strReceived.contains("JoinChordComplete")){
                String successor = arrOfStr[2].substring(24);
                String predecessor = arrOfStr[3].substring(20);
                String messages = arrOfStr[4].substring(8);
                StartProcessToCompleteJoinChord(successor,predecessor, messages);
            }
            else if(strReceived.contains("InsertRequest")){
                String message = arrOfStr[2].substring(8);
                StartProcessToInsert(message);
            }
            else if(strReceived.contains("DeleteAllRequest")){
                String originator = arrOfStr[2].substring(11);
                StartProcessToDelete(originator);
            }
            else if(strReceived.contains("GetAllRequest")){
                String originator = arrOfStr[2].substring(11);
                StartProcessToGetAll(originator);
            }
            else if(strReceived.contains("SendGetAllData")){
                String message = arrOfStr[2].substring(8);
                String completed = arrOfStr[3].substring(10);
                StartProcessToSendGetAllData(message,completed);
            }
            else if(strReceived.contains("UpdateSuccessor")){
                String successor = arrOfStr[2].substring(24);
                StartProcessToUpdateSuccessor(successor);
            }

            return;
        }

        private void StartProcessToSendGetAllData(String message, String completed){
            try{

                try {
                    messageDeliveryQueue.put(message);
                    if(completed.equals("yes")){
                        messageDeliveryQueue.put("completed");
                    }

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
        }

        private String StartProcessToGetAll(String originator){
            try{

                try {
                    Cursor result = mContentResolver.query(mUri, null,"*",new String[]{originator},null);
                    return (GetMessageFromNormalCursor(result));

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
            return null;
        }

        private String StartProcessToGet(String originator, String selection){
            try{

                try {
                    Cursor result = mContentResolver.query(mUri, null,selection,new String[]{originator},null);
                    return (GetMessageFromNormalCursor(result));

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
            return null;
        }

        private String GetMessageFromNormalCursor(Cursor cursor){
            StringBuilder result = new StringBuilder();

            int keyIndex = cursor.getColumnIndex(KEY_FIELD);
            int valueIndex = cursor.getColumnIndex(VALUE_FIELD);

            if (cursor != null) {

                cursor.moveToFirst();
                for (int i = 0; i < cursor.getCount(); i++) {
                    if(i != 0){
                        result.append("---");
                    }
                    String key = cursor.getString(keyIndex);
                    String value = cursor.getString(valueIndex);

                    result.append(key + "#" + value);

                    cursor.moveToNext();
                }
                cursor.close();
            }

            return result.toString();

        /*
        https://stackoverflow.com/questions/10081631/android-cursor-movetonext
        */
        }

        private void StartProcessToDelete(String originator){
            try{

                try {
                    mContentResolver.delete(mUri, "*",new String[]{originator});

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
        }

        private void StartProcessToInsert(String message){
            try{
                String[] arrOfStr = message.split("#",0);
                String key = arrOfStr[0];
                String value = arrOfStr[1];
                ContentValues cv = new ContentValues();
                cv.put("key", key);
                cv.put("value", value);
                try {
                    mContentResolver.insert(mUri, cv);

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
            catch (Exception ex){

            }
        }

        private void StartProcessToUpdateSuccessor(String successor){
            try{
                successorNodePortNumber = successor;
                successorNodeHash = genHash(successor);

                Log.e(TAG, "StartProcessToUpdateSuccessor suc=" + successorNodePortNumber  + " predecessorNodePortNumber=" + predecessorNodePortNumber);
            }
            catch (Exception ex){

            }
        }

        private void StartProcessToCompleteJoinChord(String successor, String predecessor, String messages){
            try{
                successorNodePortNumber = successor;
                predecessorNodePortNumber = predecessor;
                successorNodeHash = genHash(successor);
                predecessorNodeHash = genHash(predecessor);
                InsertMessagesToAvd(messages);

                Log.e(TAG, "successorNodePortNumber=" + successorNodePortNumber  + " predecessorNodePortNumber=" + predecessorNodePortNumber);

            }
            catch (Exception ex){

            }
        }

        private void InsertMessagesToAvd(String messages){

            String[] list = messages.split("---",0);

            for(String message: list){
                String[] arrOfStr = message.split("#",0);
                if(arrOfStr[0] == "")
                    break;
                String key = arrOfStr[0];
                String value = arrOfStr[1];
                ContentValues cv = new ContentValues();
                cv.put("key", key);
                cv.put("value", value);
                try {
                    mContentResolver.insert(mUri, cv);

                } catch (Exception e) {
                    Log.e(TAG, e.toString());
                }
            }
        }

        private void StartProcessToJoinChord(String port){

            try{
                String portHash = genHash(port);

                Log.e(TAG, "StartProcessToJoinChord portHash= " + portHash + " Port=" + port);

                if(successorNodeHash == "" && predecessorNodeHash == ""){

                    successorNodePortNumber = port;
                    predecessorNodePortNumber = port;
                    successorNodeHash = portHash;
                    predecessorNodeHash = portHash;
                    NotifyJoinChordComplete(port, myPort, myPort);
                }
                else if(isNodeBehind(portHash)){
                    NotifyJoinChordComplete(port, myPort, predecessorNodePortNumber);
                    NotifyChordToUpdateSuccessor(predecessorNodePortNumber, port);
                    predecessorNodeHash = portHash;
                    predecessorNodePortNumber = port;
                }
                else {
                    RequestToJoinIntoChord(port,successorNodePortNumber);
                }
            }
            catch (Exception ex){

            }
        }
    }
}
