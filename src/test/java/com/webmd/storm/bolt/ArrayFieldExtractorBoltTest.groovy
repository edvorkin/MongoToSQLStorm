package com.webmd.storm.bolt

import backtype.storm.Testing
import backtype.storm.task.OutputCollector
import backtype.storm.testing.MkTupleParam
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.tuple.Values
import com.mongodb.DBObject
import com.mongodb.util.JSON
import org.bson.types.BSONTimestamp;
import spock.lang.Specification
import backtype.storm.tuple.Tuple;
/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/27/13
 * Time: 12:18 PM
 * To change this template use File | Settings | File Templates.
 */
class ArrayFieldExtractorBoltTest extends Specification {
    private static final String ANY_NON_SYSTEM_COMPONENT_ID = "irrelevant_component_id";
    private static final String ANY_NON_SYSTEM_STREAM_ID = "irrelevant_stream_id";

    def "test that bolt extract array out of document" () {
       given:
       // backtype.storm.tuple.Tuple tuple=Mock()
        OutputCollector collector = Mock()
        ArrayFieldExtractorBolt arrayFieldExtractorBolt=new ArrayFieldExtractorBolt()
        arrayFieldExtractorBolt.outputCollector=collector

        and:
        String json = '''{

        "v" : 2,
        "op" : "i",
        "ns" : "cp.tlc",
        "o" : {

                "_class" : "com.webmd.cp.domain.UserTactic",
                "uid" : "1",
                "tacticId" : 10056,
                "active" : true,
                "assigned" : {
                        "activityId" : 10002
                },
                ads:[{"id":100,"pos":"abc"},
                     {"id":101,"pos":"def"}],
                "conditions" : {
                        "tacticid" : 10056,
                        "tacticnotes" : "Not tactic",
                        "tacticruleid" : "null"
                }
        }
}'''
        DBObject dbObject = (DBObject)JSON.parse(json);
        dbObject.put("ts", new BSONTimestamp())

        List<Object> data = new ArrayList<Object>();
        data.add(dbObject);
        // specify stream, component and fields
        MkTupleParam param = new MkTupleParam();
        //param.setStream("test-stream");
        //param.setComponent("test-component");
        param.setFields("document");
        Tuple tuple=Testing.testTuple(data,param)

        when:
        arrayFieldExtractorBolt.execute(tuple)

       then:
        1 * collector.emit("document",_)
        1 *  collector.emit("array",_);


     }
}