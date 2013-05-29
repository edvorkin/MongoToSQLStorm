package com.webmd.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/27/13
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 * breaks into
 */
public class ArrayFieldExtractorBolt extends BaseRichBolt {
    private static final long serialVersionUID = 4931640198501530202L;
    OutputCollector outputCollector;
    private Map<String, Object> arrayValue = new HashMap<String, Object>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        final List<Object> documentValues = new ArrayList();
        final BasicDBObject oplogObject = (BasicDBObject) tuple.getValueByField("document");
        final BasicDBObject document = (BasicDBObject) oplogObject.get("o");


        documentValues.add(oplogObject);   // operation from ops_log

        // if document has array fields, then send array to arraystream,
        // everything else as document stream anyway
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            if (entry.getValue() instanceof BasicDBList) {
                // emit each array elements
                BasicDBList arrayFields = (BasicDBList) entry.getValue();
                for (int i = 0; i < arrayFields.size(); i++) {
                    BasicDBObject o = (BasicDBObject)arrayFields.get(i);


                    BasicDBObject arrayVal = new BasicDBObject();

                    arrayVal.put("ns", oplogObject.getString("ns") + "_" + entry.getKey());
                    o.put("_id", document.getObjectId("_id"));
                    arrayVal.put("o", o);

                    System.out.println("list " + entry.getKey() + "/" + entry.getValue());
                    List<Object> values = new ArrayList<Object>();
                    values.add(arrayVal);
                    outputCollector.emit("documents", tuple, values);
                    //outputCollector.ack(tuple);
                    arrayValue.clear();
                }

            }
            System.out.println(entry.getKey() + "/" + entry.getValue() + "/" + entry.getValue().getClass().getName());

        }
        outputCollector.emit("documents", tuple, documentValues);
        outputCollector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
       /* List arrayFields=new ArrayList();
        arrayFields.addAll(arrayValue.keySet());
        outputFieldsDeclarer.declareStream("array",new Fields(arrayFields));*/
        outputFieldsDeclarer.declareStream("documents", new Fields("document"));
    }
}
