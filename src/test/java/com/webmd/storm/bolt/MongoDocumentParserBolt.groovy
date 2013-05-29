package com.webmd.storm.bolt

import backtype.storm.Testing
import backtype.storm.task.OutputCollector
import backtype.storm.testing.MkTupleParam
import com.mongodb.DBObject
import com.mongodb.util.JSON
import org.bson.types.BSONTimestamp;
import spock.lang.Specification

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/27/13
 * Time: 7:55 PM
 * To change this template use File | Settings | File Templates.
 */
class MongoDocumentParserBoltTest extends Specification {
       def "Flatten out mongoDB Document structure"() {
         given:"mongodb document with embedded objects and arrays"
           MongoDocumentParserBolt md=new MongoDocumentParserBolt()

           DBObject dbObject = (DBObject)JSON.parse(json);
           dbObject.put("ts", new BSONTimestamp())
         when:

             Map fields=md.flattenDocument(dbObject.get("o"))
         then:"flatten out structure"
            fields.uid=="1"
            fields.assigned_activityId ==10002
            fields.conditions_tacticid ==10056
            fields.ads== '[ { "id" : 100 , "pos" : "abc"} , { "id" : 101 , "pos" : "def"}]'
         where:
         json = '''{

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


       }
    def "Execute bolt"() {
      given:
      OutputCollector collector = Mock()
      MongoDocumentParserBolt mongoDocumentParserBolt=new MongoDocumentParserBolt()
        mongoDocumentParserBolt.outputCollector=collector;
        DBObject dbObject = (DBObject)JSON.parse(json);
        dbObject.put("ts", new BSONTimestamp())

        List<Object> data = new ArrayList<Object>();
        data.add(dbObject);
        // specify stream, component and fields
        MkTupleParam param = new MkTupleParam();
        //param.setStream("test-stream");
        //param.setComponent("test-component");
        param.setFields("document");
        backtype.storm.tuple.Tuple tuple=Testing.testTuple(data,param)

      when:
        mongoDocumentParserBolt.execute(tuple)

      then:
        1  *  collector.emit(_)
      where:
      json = '''{

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

    }
}