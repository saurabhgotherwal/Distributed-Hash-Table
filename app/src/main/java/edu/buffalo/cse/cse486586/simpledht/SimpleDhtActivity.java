package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;


public class SimpleDhtActivity extends Activity {

    static final String MASTER_PORT = "11108";
    private ContentResolver mContentResolver;
    private Uri mUri;
    private ContentValues[] mContentValues;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private TextView mTextView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        mTextView = tv;
        tv.setMovementMethod(new ScrollingMovementMethod());

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);

        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver(), portStr));

        findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mContentResolver = getContentResolver();
                mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                Cursor resultCursor = mContentResolver.query(mUri, null,"@",null,null);
                String result = GetFormatedMessageFromCursor(resultCursor);

                mTextView.append(result);
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

            private Uri buildUri(String scheme, String authority) {
                Uri.Builder uriBuilder = new Uri.Builder();
                uriBuilder.authority(authority);
                uriBuilder.scheme(scheme);
                return uriBuilder.build();
            }
        });


        findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mContentResolver = getContentResolver();
                mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                Cursor resultCursor = mContentResolver.query(mUri, null,"*",null,null);
                String result = GetFormatedMessageFromCursor(resultCursor);

                mTextView.append(result);

                //mContentResolver.delete(mUri,"@",null);
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

            private Uri buildUri(String scheme, String authority) {
                Uri.Builder uriBuilder = new Uri.Builder();
                uriBuilder.authority(authority);
                uriBuilder.scheme(scheme);
                return uriBuilder.build();
            }
        });

        boolean joinChord = false;
        if(!myPort.trim().equals(MASTER_PORT.trim())){
            joinChord = true;
        }

        SimpleDhtProvider simpleDhtProvider = new SimpleDhtProvider(portStr,joinChord,getContentResolver());
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
