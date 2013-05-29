package com.webmd.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/28/13
 * Time: 8:15 AM
 * To change this template use File | Settings | File Templates.
 */
public class SQLWriterBolt extends BaseRichBolt {
    protected static transient Connection con = null;
    static Logger LOG = Logger.getLogger(SQLWriterBolt.class);
    protected String dBUrl = null;
    protected String username = null;
    protected String password = null;
    OutputCollector outputCollector;

    public SQLWriterBolt(String dBUrl, String username, String password) {
        this.dBUrl = dBUrl;
        this.username = username;
        this.password = password;
    }

    // for unit testing only
    public SQLWriterBolt() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
         LOG.info("try to load the vertica JDBC driver with " + dBUrl);
        try {
            Class.forName("com.vertica.jdbc.Driver");
            LOG.info("loaded vertica driver");

            con = DriverManager.getConnection(dBUrl, username, password);
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String insertStatement=createInsertStatement(tuple);
        try {
            Statement stmt = con.createStatement();
            stmt.execute(insertStatement);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }


     String createInsertStatement(Tuple tuple) {
      StringBuffer insert_sql=new StringBuffer("INSERT INTO ");
        StringBuffer columns=new StringBuffer();
        StringBuffer values=new StringBuffer();

        String tableName=tuple.getStringByField("ns");
        insert_sql.append(tableName);
        Map<String,Object> fields=(Map<String,Object>)tuple.getValueByField("fields");
        // for each field in the map create values
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            System.out.println("list " + entry.getKey() + "/" + entry.getValue());
            columns.append(entry.getKey()+",");
            if (entry.getValue() instanceof String) {
                values.append("'"+entry.getValue()+"',");
            } else {
                values.append(entry.getValue()+",");
            }

        }
        // remove training commas
        insert_sql.append(" (" + columns.substring(0, columns.length() - 1) + ")");
        insert_sql.append(" VALUES (" + values.substring(0, values.length() - 1)+")");
        return insert_sql.toString();
    }
}
