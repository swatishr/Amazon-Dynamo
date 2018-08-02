package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;

/**
 * Created by swati on 4/25/18.
 */

public class DataMessage extends Message{
    String key;
    String value;
    Cursor cursor;

    public DataMessage(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public DataMessage(String msgType, String originNode, int destNode, String key, String value) {
        super(msgType, originNode, destNode);
        this.key = key;
        this.value = value;
    }

    public DataMessage(String msgType, String originNode, int destNode, Cursor result) {
        super(msgType, originNode, destNode);
        this.key = "";
        this.value = "";
        this.cursor = result;
    }

    @Override
    public String toString() {

        //convert Cursor object to a string of keys and values and store it in key and value instance variables
        if(cursor != null){
            StringBuilder keystr = new StringBuilder();
            StringBuilder valuestr = new StringBuilder();
            cursor.moveToFirst();
            do {
                String key1 = cursor.getString(0);
                String value1 = cursor.getString(1);
                keystr.append(key1);keystr.append(":");
                valuestr.append(value1);valuestr.append(":");
            }while(cursor.moveToNext());
            keystr.deleteCharAt(keystr.length()-1);
            valuestr.deleteCharAt(valuestr.length()-1);

            key = keystr.toString();
            value = valuestr.toString();
        }

        return  msgType + "#" + originNode + "#" + destNode + "#" + key + "#" + value;
    }
}
