package com.webmd.storm

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.webmd.storm.bolt.ArrayFieldExtractorBolt
import com.webmd.storm.bolt.MongoDocumentParserBolt
import com.webmd.storm.bolt.SQLWriterBolt
import com.webmd.storm.spout.MongoOpLogSpout
import com.webmd.storm.util.OpLogManager;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/28/13
 * Time: 11:45 AM
 * To change this template use File | Settings | File Templates.
 */

def config = new ConfigSlurper().parse(new File('mongodbverticatopology.conf').toURI().toURL())
String rdbmsUrl = config.rdbmsUrl
String rdbmsUserName =config.rdbmsUserName
String rdbmsPassword = config.rdbmsPassword
String opslog_progress=config.opslog_progress
Mongo mongo = new Mongo(config.server, config.port as int)

// topology elements id's
String spoutId = "mongodbOpLog";
String arrayExtractorId = "arrayExtractor";
String mongoDocParserId = "mongoDocParser";
String sqlWriterId = "sqlWriter";
String documentsStreamId="documents"

OpLogManager opLogManager=new OpLogManager()
DBObject query = opLogManager.findLastOpLogEntry(mongo, config.opslog_progress);

// build topology
TopologyBuilder builder = new TopologyBuilder();
// define our spout
builder.setSpout(spoutId, new MongoOpLogSpout("mongodb://${config.server}:${config.port}/local", query, ["cp.events","cp.tlc"], opslog_progress),1)
builder.setBolt(arrayExtractorId ,new ArrayFieldExtractorBolt()).shuffleGrouping(spoutId)
builder.setBolt(mongoDocParserId, new MongoDocumentParserBolt()).shuffleGrouping(arrayExtractorId,documentsStreamId)
builder.setBolt(sqlWriterId, new SQLWriterBolt(rdbmsUrl,rdbmsUserName,rdbmsPassword)).shuffleGrouping(mongoDocParserId)

// Set debug config
Config conf = new Config();
conf.setMessageTimeoutSecs(20)
conf.setDebug(Boolean.parseBoolean(config.debug));

LocalCluster cluster = new LocalCluster();
cluster.submitTopology("test", conf, builder.createTopology());


