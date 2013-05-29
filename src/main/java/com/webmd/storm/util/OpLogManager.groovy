package com.webmd.storm.util

import com.mongodb.*
import com.mongodb.util.JSON
import org.bson.types.BSONTimestamp

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 2/28/13
 * Time: 9:11 AM
 * To change this template use File | Settings | File Templates.
 */
class OpLogManager {
    /* A dictionary that stores OplogThread/timestamp pairs.
  Represents the last checkpoint for a OplogThread.*/
    def oplog_progress = [:]
    def timestamp, inc
    private String oplog_checkpoint;
    //Stores the timestamp of the last oplog entry read.
    def checkpoint = null


    DBObject findLastOpLogEntry(Mongo mongo, String opslog) throws UnknownHostException {

        DB db = mongo.getDB("local");
        DBObject query = null;
        // Connect to the db and find the current last timestamp
        def timestamp=read_oplog_progress(opslog)
        if (timestamp!=null) {
            query = new BasicDBObject("ts", new BasicDBObject('$gt', timestamp));
        } else {


            DBCursor cursor = db.getCollection('oplog.rs').find().sort(new BasicDBObject('$natural', -1)).limit(1);
            if(cursor.hasNext()) {
                // Get the next object
                DBObject object = cursor.next();
                // Build the query
                query = new BasicDBObject("ts", new BasicDBObject('$gt', object.get("ts")));
            }
        }
        // Return the query to find the last op log entry
        return query;
    }


    /*"""Dumps collection into the target system.

        This method is called when we're initializing the cursor and have no
        configs i.e. when we're starting for the first time.
        """*

        */
    def BasicDBObject dumpCollection(List dumpset,Mongo mongo, DB db) {
        // get last timestamp and create DBObject
        // idea is to simply dump all data into Vertica and then start threads
        // Get db
        //TODO: get last ops timestamp

        dumpset { namespace ->
            DBCollection collection = db.getCollection(namespace);
            DBCursor cursor=collection.find()
            //TODO: long?
            def longTimestamp=timestamp
            while(cursor.hasNext()) {
                BasicDBObject doc=cursor.next()
                // create a spout that reads from cursor

            }
        }

    }


//DBObject initCursor(Mongo mongo) throws UnknownHostException
/*"""Position the cursor appropriately.

    The cursor is set to either the beginning of the oplog, or
    wherever it was last left off.
    """*/

def updateCheckpoint() {
    //"""Store the current checkpoint in the oplog progress dictionary.



}


def readLastCheckpoint() {


    //"""Read the last checkpoint from the oplog progress dictionary.


}

/*Reads oplog progress from file provided by user.
This method is only called once before any threads are spanwed.
"""*/
BSONTimestamp read_oplog_progress(String oplog_checkpoint){
    if (oplog_checkpoint==null) {
        return null;
    }

    def f1=new File(oplog_checkpoint)
    if (f1.size()==0) {
        println ("MongoConnector: Empty oplog progress file.")
        return null;
    }
    timestamp=f1.getText()
    BSONTimestamp ts=JSON.parse(timestamp)
    return ts



}
}



