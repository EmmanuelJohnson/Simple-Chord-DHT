package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        final Button LDump = (Button) findViewById(R.id.button1);
        final Button GDump = (Button) findViewById(R.id.button2);

        LDump.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                tv.append("@@@\n");
                try {
                    Cursor resultCursor = getContentResolver().query(mUri, null,
                            "@", null, null);
                    if (resultCursor == null) {
                        Log.e(TAG, "Result null");
                        throw new Exception();
                    }
                    resultCursor.moveToFirst();
                    for (int i = 0; i < resultCursor.getCount(); i++) {

                        String strReceived = "K->"+resultCursor.getString(resultCursor.getColumnIndex(KEY_FIELD))+"\nV->"
                                +resultCursor.getString(resultCursor.getColumnIndex(VALUE_FIELD))+"\n@@@";
                        tv.append(strReceived + "\n");
                        resultCursor.moveToNext();
                    }
                }
                catch (Exception e) {
                    Log.e("buttonClick", "LDUMP ERROR: "+e);
                }
            }
        });

        GDump.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                tv.append("***\n");
                new GTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    private class GTask extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            try {
                Cursor resultCursor = getContentResolver().query(mUri, null,
                        "*", null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }
                resultCursor.moveToFirst();
                for (int i = 0; i < resultCursor.getCount(); i++) {
                    String strReceived = "K->"+resultCursor.getString(resultCursor.getColumnIndex(KEY_FIELD))+"\nV->"
                            +resultCursor.getString(resultCursor.getColumnIndex(VALUE_FIELD))+"\n***";
                    publishProgress(strReceived);
                    resultCursor.moveToNext();
                }
            }
            catch (Exception e) {
                Log.e("buttonClick", "GDUMP ERROR: "+e);
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(strReceived + "\n");
            return;
        }
    }

}
