package com.webmd.storm.spout

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.tuple.Fields
import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.webmd.storm.util.MongoObjectGrabber
import groovy.util.logging.Log4j
import org.apache.log4j.Logger
import org.bson.BSONObject
import org.bson.types.BSONTimestamp


@Log4j
public class MongoOpLogSpout extends MongoSpoutBase implements Serializable {

    private static final long serialVersionUID = 5498284114575395939L;
    static Logger LOG = Logger.getLogger(MongoOpLogSpout.class);

    private static String[] collectionNames = ['oplog.$main', "oplog.rs"];
    private List<String> filterByNamespace;

    private String oplog_checkpoint;
    private Map opsLogProgressMap
    private static final Integer MAX_FAILS = 2;
    Map<String, DBObject> messages;
    Map<Integer, Integer> transactionFailureCount;
    Map<Integer, String> toSend;





    public MongoOpLogSpout(String url, DBObject query, List<String> filterByNamespace, String checkpoint) {

        super(url, "local", collectionNames, query, null);
        this.oplog_checkpoint = checkpoint
        this.filterByNamespace = filterByNamespace;
    }



    @Override
    protected void processNextTuple() {
        DBObject object = this.queue.poll();
        // If we have an object, let's process it, map and emit it
        if (object != null) {
            String operation = object.get("op").toString();
            // Check if it's a i/d/u operation and push the data
            if (operation.equals("i") || operation.equals("d") || operation.equals("u")) {
                if (LOG.isInfoEnabled()) LOG.info(object.toString());

                // Verify if it's the correct namespace
                if (this.filterByNamespace != null && !this.filterByNamespace.contains(object.get("ns").toString())) {
                    return;
                }

                // Map the object to a tuple
                List<Object> tuples = this.mapper.map(object);
                // Contains the objectID
                String objectId = null;
                // Extract the ObjectID -- ignore delete operations
                //  || operation.equals("d")  this is removed
                if (object.get("o") != null && ((BSONObject) object.get("o")).get("_id") != null) {

                    objectId = ((BSONObject) object.get("o")).get("_id").toString();
                    this.collector.emit(tuples, objectId);
                    // put in local meassage list for retry later on
                    messages.put(objectId, object)


                }

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Set the declaration
        declarer.declare(new Fields("document"));
    }

    @Override
    void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector)
        messages = new HashMap<Integer, String>();
        toSend = new HashMap<Integer, String>();
        transactionFailureCount = new HashMap<Integer, Integer>();
    }

    @Override
    void ack(Object msgId) {
        LOG.info("processing messageId [" + msgId + "]");
        DBObject object = messages.get(msgId)
        // write to flat file to track progress
        if (object != null) {

            write_oplog_progress(object.get('ts'))

            messages.remove(msgId);
        }
        LOG.info("Message fully processed [" + msgId + "]");
    }

    /*If a transaction fails, resend the message.
    If the transaction fails too many times, terminate the topology.*/

    @Override
    void fail(Object msgId) {
        // fail fast
        log.error("Failed for objectId: " + msgId)
        Integer failures;
        String transactionId = (String) msgId;
        log.error("Failed for object:" + messages.get(transactionId))
        if (!transactionFailureCount.containsKey(msgId)) {
            //Get the transactions fail
            failures = 1;
        } else {
            failures = transactionFailureCount.get(transactionId) + 1;
        }


        if (failures >= MAX_FAILS) {
            //If exceeds the max fails will go down the topology
            throw new RuntimeException("Error, transaction id [" + transactionId + "] has had many errors [" + failures + "]");
        }
        //If not exceeds the max fails we save the new fails quantity and re-send the message
        //by putting it PushbackInputStream to queue
        transactionFailureCount.put(transactionId, failures);

        // send message again for processing
        queue.put(messages.get(transactionId));
        LOG.info("Re-sending message [" + msgId + "]");
    }

    /* """ Writes oplog progress to file provided by user
        """*/

    public void write_oplog_progress(BSONTimestamp ts) {
        // write to temp file
        def backup_file = this.oplog_checkpoint + '.backup'
        def f1 = new File(oplog_checkpoint)
        f1.renameTo(new File(backup_file))
        // for each of the spouts write to file

        println f1.absolutePath
        f1.write(JSON.serialize(ts));
        // remove backup
        new File(backup_file).delete()
    }

}
