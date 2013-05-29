package com.webmd.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/27/13
 * Time: 8:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class MongoDocumentParserBolt extends BaseRichBolt {
    Deque<String> stack = new ArrayDeque<String>();
    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, Object> fields = new HashMap<String, Object>();
        List<Object> outputValues = new ArrayList<Object>();
        final BasicDBObject oplogObject = (BasicDBObject) tuple.getValueByField("document");
        final BasicDBObject document = (BasicDBObject) oplogObject.get("o");
        outputValues.add(oplogObject.getString("ns").replace(".","_"));
        outputValues.add(oplogObject.getString("op"));
        outputValues.add(flattenDocument(document));
        outputCollector.emit(tuple,outputValues);
        outputCollector.ack(tuple);
        // get all fields

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("ns", "op", "fields"));
    }

    Map<String, Object> flattenDocument(BasicDBObject document) {
        Map<String, Object> flattenedDocument = new HashMap<String, Object>();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            Object value = null;
            if (entry.getValue() instanceof BasicDBObject) {
                //embedded document, save field name
                stack.addFirst(entry.getKey());
                flattenedDocument.putAll(flattenDocument(((BasicDBObject) entry.getValue())));
                stack.removeFirst();
            } else if (entry.getValue() instanceof BasicDBList) {
                flattenedDocument.put(entry.getKey(), entry.getValue().toString());
            } else {

                String parent = stack.peekFirst();

                // date values
                if (entry.getValue() instanceof Date) {
                    value = new java.sql.Timestamp(((Date) entry.getValue()).getTime()).toString();
                }
                else if (entry.getValue() instanceof Double ){
                    value = entry.getValue();
                } else {
                    value = entry.getValue().toString();
                }
                if (parent != null) {
                    flattenedDocument.put(parent + "_" + entry.getKey(), value);
                } else
                    flattenedDocument.put(entry.getKey(), value);
            }

        }
        return flattenedDocument;
    }
}