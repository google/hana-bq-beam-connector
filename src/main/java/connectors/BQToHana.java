package connectors; 

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import com.google.gson.Gson;
import com.google.common.collect.ImmutableMap;
import java.security.SecureRandom;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


public class BQToHana {
    private static Logger logger = Logger.getLogger(BQToHana.class.getSimpleName());
    private static final String JOB_NAME_PREFIX = "-";
    private static final String BUCKET_NAME = "gs://mshalda-test/";
    private static final String TEMP_LOCATION = "gs://mshalda-test/temp";
    private static final String RAW_LOGS_BIG_QUERY_TABLE_PREFIX = "mshalda_test_ds.";
    private static final String HANA_CONNECTION_STRING = "jdbc:sap://10.128.15.195:30015/?databaseName=HD1";
    private static final String HANA_USER = "system";
    private static final String HANA_PW = "manager";
    private static final Gson GSON = new Gson();
    private static Map<String, Object> SAMPLE_MAP = new HashMap<String, Object>();
    private static String table_name = "MY_DATA";
    private static String value_sep = "<<>>";
    private static String col_sep = "::";
    private static final Map<String, String> hanaToBqTypeMap = ImmutableMap.<String, String>builder()
     .put("NVARCHAR", "STRING")
     .put("VARCHAR", "STRING")
     .put("ALPHANUM", "STRING")
     .put("SHORTTEXT", "STRING")
     .put("BLOB", "STRING")
     .put("CLOB", "STRING")
     .put("NCLOB", "STRING")
     .put("TEXT", "STRING")
     .put("VARBINARY", "BYTES")
     .put("INTEGER", "INTEGER")
     .put("DATE", "DATE")
     .put("TIME", "TIME")
     .put("DATETIME", "DATETIME")
     .put("BOOLEAN", "BOOLEAN")
     .put("TINYINT", "INTEGER")
     .put("SMALLINT", "INTEGER")
     .put("BIGINT", "INTEGER")
     .put("SMALLDECIMAL", "FLOAT")
     .put("DECIMAL", "FLOAT")
     .put("DOUBLE", "FLOAT")
     .put("REAL", "FLOAT")
     .build(); 



  static class PrepareStatementFromHanaRow
      implements JdbcIO.PreparedStatementSetter<String> {
    @Override
    public void setParameters(String element, PreparedStatement statement)
        throws SQLException {
      String[] col_values = element.split(col_sep);
      int i = 1;
      for(String cols : col_values)
      {
        statement.setString(i, cols);
        if(i == 15)
          break;
      }
    }
  }

    public static class InsertTableRowsInHanaFn extends PTransform<PCollection<TableRow>, PDone> {
        String table_name = "";
      
        public InsertTableRowsInHanaFn(String table){
          this.table_name = table;
        }

        public PDone expand(PCollection<TableRow> input)
        {
          Pipeline pipeline = input.getPipeline();
          input.apply(ParDo.of(new DoFn<TableRow, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              TableRow row = c.element();
              String result = "";
              String[] columns = {"title", "id", "language", "wp_namespace", "is_redirect", "revision_id", "contributor_ip", "contributor_id", "contributor_username", "timestamp", "is_minor", "is_bot", "reversion_id", "comment", "num_characters"};
              for(String col_name : columns) 
              {
                result += row.get(col_name) + col_sep;
              }
              c.output(result);
            }
          }))
            .apply(JdbcIO.<String>write()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                "com.sap.db.jdbc.Driver", HANA_CONNECTION_STRING)
              .withUsername(HANA_USER)
              .withPassword(HANA_PW))
            .withStatement(String.format("insert into %s values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", "wikipedia"))
            .withPreparedStatementSetter(new PrepareStatementFromHanaRow())); 
          return PDone.in(input.getPipeline());
        }
    }





    public static void createTable(Connection conn, String[] col_names, String[] col_types) {
      try
      {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE wikipedia");
      } catch (SQLException e) {
        System.err.println("Issue dropping table: " + e.getMessage());
      }
      try
      {
        Statement stmt = conn.createStatement();
        String create_statement = "CREATE TABLE wikipedia ("; 
        for(int i = 0; i < col_names.length; i++)
        {
          create_statement += "\"" + col_names[i] + "\" " + col_types[i];
          if(i + 1 >= col_names.length)
            create_statement += ");";
          else
            create_statement += ", ";
        }
        System.out.println(create_statement);
        stmt.executeUpdate(create_statement);
      } catch (SQLException e) {
        System.err.println("Issue creating table: " + e.getMessage());
      }
    }


    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static SecureRandom rnd = new SecureRandom();

    public static String randomString( int len ){
      StringBuilder sb = new StringBuilder( len );
      for( int i = 0; i < len; i++ ) 
        sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
      return sb.toString();
    }

    public static void populateTable(Connection conn) {
      int i = 0;
      try
      {
        while (i < 100)
        {
          String user_name = randomString(8);
          String text = randomString(100);
          String insert = "insert into " + table_name + " (ID, USER_NAME, TEXT) VALUES ('" + i + "', '"+user_name+"','"+text+"')";
          System.out.println(insert);
          Statement stmt = conn.createStatement();
          stmt.executeUpdate(insert);
          i++;
        }        
      } catch(SQLException e) {
        System.err.println("Exception inserting: " + e.getMessage());
      }

    } 
    
    public static TableSchema getSchema(String table) {
      Connection connection = null;
          try {                  
            connection = DriverManager.getConnection(HANA_CONNECTION_STRING, HANA_USER, HANA_PW);
          } catch (SQLException e) {
            System.err.println("Connection Failed. User/Passwd Error? " + e.getMessage());
          }
          if (connection != null) {
            try {
              System.out.println("Connection to HANA successful!");
              Statement stmt = connection.createStatement();
        
              ResultSet resultSet = stmt.executeQuery("SELECT COLUMN_NAME,DATA_TYPE_NAME FROM TABLE_COLUMNS WHERE TABLE_NAME = '" + table + "'");
              Map<String, String> columns = new HashMap<String,String>(); 
              while(resultSet.next())
              {
                String col_name = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("DATA_TYPE_NAME");
                columns.put(col_name, type);
                System.out.println(col_name + " <> " + type);
              } 
              TableSchema schema = new TableSchema();
              List<TableFieldSchema> tableFieldSchema = new ArrayList<TableFieldSchema>();
              for(String col_name : columns.keySet())
              {
                TableFieldSchema schemaEntry = new TableFieldSchema();
                schemaEntry.setName(col_name);
                String type = columns.get(col_name); 
                if(hanaToBqTypeMap.containsKey(type))
                  schemaEntry.setType(hanaToBqTypeMap.get(type));
                else
                  schemaEntry.setType("STRING");
                tableFieldSchema.add(schemaEntry);
              }
              schema.setFields(tableFieldSchema);
              return schema;
            }
            catch(SQLException e) {
              System.err.println("Query failed: " + e.getMessage());
            }
          }
          return null;
        }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: BQToHana <project_number> <project_name> <dataset> <table>");
            return;
        }
        final PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName(JOB_NAME_PREFIX + args[0]);
//        options.setProject("google.com:cpe-data");
        options.setTempLocation(TEMP_LOCATION);
        //options.setStreaming(true);
        //options.setNumWorkers(100);
//        options.setRunner(DirectRunner.class);
        String[] columns = {"title", "id", "language", "wp_namespace", "is_redirect", "revision_id", "contributor_ip", "contributor_id", "contributor_username", "timestamp", "is_minor", "is_bot", "reversion_id", "comment", "num_characters"};
        String[]  types = {"NVARCHAR(5000)", "INTEGER", "NVARCHAR(100)", "INTEGER", "BOOLEAN", "INTEGER", "NVARCHAR(100)", "INTEGER", "NVARCHAR(100)", "INTEGER", "BOOLEAN", "BOOLEAN", "INTEGER", "NVARCHAR(5000)", "INTEGER"};
        final Pipeline pipeline = Pipeline.create(options);
        Connection connection = null;
        try {                  
            connection = DriverManager.getConnection(HANA_CONNECTION_STRING, HANA_USER, HANA_PW);
          } catch (SQLException e) {
            System.err.println("Connection Failed. User/Passwd Error? " + e.getMessage());
          }
          if (connection != null) {
            createTable(connection, columns, types);
          }
       

        String dataset = args[2];
        //final TableSchema bqHanaSchema = getSchema(args[2]); 
        pipeline 
            .apply(BigQueryIO.read().from("bigquery-public-data:samples.wikipedia"))
            .apply(new InsertTableRowsInHanaFn("wikipedia"));
        pipeline.run();
   } 
}
