package com.webmd.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.mongodb.*;
import com.webmd.storm.util.MongoObjectGrabber;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class MongoSpoutBase extends BaseRichSpout {
    static Logger LOG = Logger.getLogger(MongoSpoutBase.class);

    protected static MongoObjectGrabber wholeDocumentMapper = null;

    // Hard coded static mapper for whole document map
    static {
        wholeDocumentMapper = new MongoObjectGrabber() {
            @Override
            public List<Object> map(DBObject object) {
                List<Object> tuple = new ArrayList<Object>();
                tuple.add(object);
                return tuple;
            }

            @Override
            public String[] fields() {
                return new String[]{"document"};
            }
        };
    }

    // Internal state
    private String dbName;
    private DBObject query;
    protected MongoObjectGrabber mapper;
    protected Map<String, MongoObjectGrabber> fields;

    // Storm variables
    protected Map conf;
    protected TopologyContext context;
    protected SpoutOutputCollector collector;

    // Handles the incoming messages
    protected LinkedBlockingQueue<DBObject> queue = new LinkedBlockingQueue<DBObject>(10000);
    private String url;
    private MongoSpoutTask spoutTask;
    private String[] collectionNames;


    // constructor for testing only

    protected MongoSpoutBase() {

    }

    public MongoSpoutBase(String url, String dbName, String[] collectionNames, DBObject query, MongoObjectGrabber mapper) {
        this.url = url;
        this.dbName = dbName;
        this.collectionNames = collectionNames;
        this.query = query;
        this.mapper = mapper == null ? wholeDocumentMapper : mapper;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Set the declaration
        declarer.declare(new Fields(this.mapper.fields()));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // Save parameters from storm
        this.conf = conf;
        this.context = context;
        this.collector = collector;
        // get cursor, where we left of, for this executor
        // get this task id
        //this.query=getOpsLogQuery(context.getThisComponentId());
        // Set up an executor
        // if clustered sharded setup, use multiple threads
        // connect to mongoS first and get sharded configuration information
        boolean sharded=isSharded();
        MongoURI uri = new MongoURI(this.url);
        Mongo mongo = null;
        ArrayList<String>  hosts=new ArrayList<String>();
        try {
            mongo = new Mongo(uri);
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }





        if (sharded) { // get each server and port info from shards collection
            DB db = mongo.getDB("config");
            DBCursor cursor=db.getCollection("shards").find();
            while( cursor.hasNext() )  {
                DBObject obj = cursor.next();
                String host=obj.get("host").toString();
                String replSet=host.split("/")[0];
                hosts.add(host.split("/")[1]);
                List addrs = new ArrayList();
                for (String replicaSetHosts: hosts) {

                    String[] individualHost= replicaSetHosts.split(",");
                    for (int i = 0; i < individualHost.length ; i++) {
                        String[] HostPort = individualHost[i].split(":");

                        try {
                            addrs.add(new ServerAddress( HostPort[0],Integer.parseInt(HostPort[1])) );
                        } catch (UnknownHostException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }


                    }
                    // create a thread for each ReplicaSet in the shard
                    this.spoutTask = new MongoSpoutTask(this.queue, addrs, this.dbName, this.collectionNames, this.query);
                    // Start thread
                    Thread thread = new Thread(this.spoutTask);
                    thread.start();

                    addrs.clear();
                }
            }
        }else {
            // there is just simple replica set configuration
            // replica set information stored in
            List addrs = new ArrayList();
            DB db = mongo.getDB("admin");
            DBObject cmd = new BasicDBObject();
            cmd.put("replSetGetStatus", 1);
            CommandResult result = db.command(cmd);
            ArrayList<BasicDBObject> members= (ArrayList<BasicDBObject>) result.get("members");
            for (int i = 0; i < members.size(); i++) {
                BasicDBObject o =  members.get(i);
                String[] hostPort=o.getString("name").split(":");
                try {
                    addrs.add( new ServerAddress( hostPort[0] , Integer.parseInt(hostPort[1]) ) );
                } catch (UnknownHostException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
            // create a thread with set of addresses
            // create a thread for each ReplicaSet in the shard
            this.spoutTask = new MongoSpoutTask(this.queue, addrs, this.dbName, this.collectionNames, this.query);
            // Start thread
            Thread thread = new Thread(this.spoutTask);
            thread.start();


        }






    }

    @Override
    public void close() {
        // Stop the thread
        this.spoutTask.stopThread();
    }

    protected abstract void processNextTuple();

    @Override
    public void nextTuple() {
        processNextTuple();
    }

    @Override
    public void ack(Object msgId) {
        LOG.info("message in ask  " + msgId);
        LOG.info("timestamp"+ new ObjectId(msgId.toString()).getTime());

    }

    @Override
    public void fail(Object msgId) {
    }

    // check if database has sharded setup
    //by executing isdbgrid command
    private boolean isSharded() {
        MongoURI uri = new MongoURI(this.url);
        Mongo mongo = null;
        try {
            mongo = new Mongo(uri);
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        DB db = mongo.getDB("admin");
        DBObject cmd = new BasicDBObject();
        cmd.put("isdbgrid", 1);
        CommandResult result = db.command(cmd);
        if (result.containsField("isdbgrid")) {
           if (result.getInt("isdbgrid")==1) return true;
        }
        return false;
    }


}
