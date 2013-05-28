package com.webmd.storm.bolt

import backtype.storm.Testing
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.testing.MkTupleParam
import org.bson.types.ObjectId;
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import groovy.sql.Sql

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/28/13
 * Time: 8:07 AM
 * To change this template use File | Settings | File Templates.
 */
class SQLWriterBoltTest extends Specification {
    /*static transient*/ Connection con = null;
        def "Opens connection to database"() {
          given:

            SQLWriterBolt sqlWriterBolt=new SQLWriterBolt(rdbmsUrl,rdbmsUserName,rdbmsPassword)
            Map map=Mock()
            TopologyContext topologyContext=Mock()
            OutputCollector outputCollector=Mock()
            Class.forName("com.vertica.jdbc.Driver");



          when:
            //
          con=DriverManager.getConnection(rdbmsUrl, rdbmsUserName, rdbmsPassword);

            println con
          then:
              notThrown(SQLException)
              //sqlWriterBolt.con!=null
          cleanup:
          con.close()
            //sqlWriterBolt.con.close()
          where:

              rdbmsUrl="jdbc:vertica://vrtcl01d-shr-08.portal.webmd.com/ydvorkin"
              rdbmsUserName= 'ydvorkin'
              rdbmsPassword= 'ydvorkin123'

        }

    def "Create insert statement"() {
      given:
      List<Object> data = new ArrayList<Object>();
      def fields=["_id":"123","some_int": 10,"some_boolean":true]
      data.add("cp.events")
      data.add(fields)
      MkTupleParam param = new MkTupleParam();
        //param.setStream("test-stream");
        //param.setComponent("test-component");
        param.setFields("ns","fields");
        backtype.storm.tuple.Tuple tuple=Testing.testTuple(data,param)
        SQLWriterBolt sqlWriterBolt=new SQLWriterBolt()
      when:
      String sql=sqlWriterBolt.createInsertStatement(tuple)

      then:
       sql=="INSERT INTO cp.events (_id,some_int,some_boolean) VALUES ('123',10,true)"


    }

    def "Insert record into database integration test"() {
      given:
        SQLWriterBolt sqlWriterBolt=new SQLWriterBolt(rdbmsUrl,rdbmsUserName,rdbmsPassword)
        Map map=Mock()
        TopologyContext topologyContext=Mock()
        OutputCollector outputCollector=Mock()
        sqlWriterBolt.prepare(map,topologyContext,outputCollector)
      and: "tuple exists"
        List<Object> data = new ArrayList<Object>();
        data.add("events")
        data.add(fields)
        MkTupleParam param = new MkTupleParam();
        param.setFields("ns","fields");
        backtype.storm.tuple.Tuple tuple=Testing.testTuple(data,param)
      when:
        sqlWriterBolt.execute(tuple)
        // prepare for validation
        def sql = Sql.newInstance( rdbmsUrl, rdbmsUserName,
                rdbmsPassword, "com.vertica.jdbc.Driver" )
        def row = sql.firstRow("select _id, uid from events where _id='"+id+"'")

      then:"record in inserted"

        row!=null
      cleanup:
        sql.execute("delete from events where _id='"+id+"'")
      where:


        rdbmsUrl="jdbc:vertica://vrtcl01d-shr-08.portal.webmd.com/ydvorkin"
        rdbmsUserName= 'ydvorkin'
        rdbmsPassword= 'ydvorkin123'
        id= new ObjectId().toString();
        fields=["_id":id,"uid": 10222111]


    }
}