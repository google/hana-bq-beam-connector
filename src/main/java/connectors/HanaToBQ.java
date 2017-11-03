/*
 * Copyright 2017 Mark Shalda 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
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
import connectors.DBRow;

public class HanaToBQ {
  private static Logger logger = Logger.getLogger(HanaToBQ.class.getSimpleName());
  private static Map<String, Object> SAMPLE_MAP = new HashMap<String, Object>();
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

  public static class ScanHanaTableFn extends PTransform<PBegin, PCollection<TableRow>> {

    String driver = "";
    String conn_string = "";
    String username = "";
    String password = "";
    String query = "";
    public ScanHanaTableFn(String d, String cs, String u, String p, String q) 
    {
      this.driver = d;
      this.conn_string = cs;
      this.username = u;
      this.password = p;
      this.query = q;
    }

    public PCollection<TableRow> expand(PBegin input)
    {
      Pipeline pipeline = input.getPipeline();

      return pipeline.apply("Map Hana to DBRow", JdbcIO.<DBRow>read()
          .withCoder(SerializableCoder.of(DBRow.class))
          .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
              this.driver, this.conn_string)
            .withUsername(this.username)
            .withPassword(this.password))

          .withQuery(this.query)
          .withRowMapper(new JdbcIO.RowMapper<DBRow>() {
            public DBRow mapRow(ResultSet resultSet) throws Exception {
              ResultSetMetaData meta = resultSet.getMetaData();
              int columnCount = meta.getColumnCount();
              List<Object> values = new ArrayList<Object>();
              List<String> col_names = new ArrayList<String>();
              List<Integer> col_types = new ArrayList<Integer>(); 
              for (int column = 1; column <= columnCount; ++column) 
              {
                Object value = resultSet.getObject(column);
                values.add(value);
                col_names.add(meta.getColumnName(column));
                col_types.add(meta.getColumnType(column));
              }
              return DBRow.create(values, col_names, col_types);
            }
          }))
      .apply("DBRow to TableRow", ParDo.of(new DoFn<DBRow, TableRow>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          DBRow data = c.element();
          List<Object> fields = data.fields();
          List<String> col_names = data.col_names();
          TableRow row = new TableRow();
          for(int i = 0; i < fields.size(); i++)
          {
            Object field_data = fields.get(i);
            String col_name = col_names.get(i);
            if(field_data == null)
              continue;
            String s_data = field_data.toString();
            if(!s_data.toLowerCase().equals("null"))
              row.put(col_name, s_data);
          }
          c.output(row);
        }
      }));
    }
  }


  /**
   * Retrieves the schema from a hana database table and converts it to BigQuery TableSchema object.
   * @param table table name in the database.
   * @return TableSchema for the table
   */
  public static TableSchema getSchema(Options options) {
    Connection connection = null;
    try {                  
      connection = DriverManager.getConnection("jdbc:sap://10.128.15.195:30015/?databaseName=HD1", options.getUsername(), options.getPassword());
    } catch (SQLException e) {
      System.err.println("Connection Failed while trying to retrieve schema: " + e.getMessage());
    }
    if (connection != null) {
      try {
        Statement stmt = connection.createStatement();

        ResultSet resultSet = stmt.executeQuery("SELECT COLUMN_NAME,DATA_TYPE_NAME FROM TABLE_COLUMNS WHERE TABLE_NAME = '" + options.getTableName() + "'");
        Map<String, String> columns = new HashMap<String,String>(); 
        while(resultSet.next())
        {
          String col_name = resultSet.getString("COLUMN_NAME");
          String type = resultSet.getString("DATA_TYPE_NAME");
          columns.put(col_name, type);
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
        System.err.println("Query failed while getting schema: " + e.getMessage());
      }
    }
    return null;
  }

  public interface Options extends PipelineOptions {
    @Description("Hana table name to read")
      @Validation.Required
      String getTableName();
    void setTableName(String value);

    @Description("Hana connection string.")
      @Validation.Required
      String getConnectionString();
    void setConnectionString(String value);

    @Description("Hana username.")
      @Validation.Required
      String getUsername();
    void setUsername(String value);

    @Description("Hana password.")
      @Validation.Required
      String getPassword();
    void setPassword(String value);

    @Description("Hana driver to use.")
      @Default.String("com.sap.db.jdbc.Driver")
      String getDriver();
    void setDriver(String value);

    @Description("Bigquery destionation dataset.")
      @Validation.Required
      String getDestDataset();
    void setDestDataset(String value);

    @Description("Timestamp column name.")
      @Validation.Required
      String getTimestampColumn();
    void setTimestampColumn(String value);

    @Description("Start time, inclusive")
      @Validation.Required
      String getStartTime();
    void setStartTime(String value);

    @Description("End time, inclusive")
    String getEndTime();
    void setEndTime(String value);
  }


  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);
    TableSchema bqHanaSchema = getSchema(options); 
    String query = "SELECT * FROM " + options.getTableName() + " WHERE " +
      options.getTimestampColumn() + " >= " + options.getStartTime();
    if(options.getEndTime() != null)
    {
      query += " AND " + options.getTimestampColumn() + " < " + options.getEndTime();
    }
    logger.info(query);
    pipeline 
      .apply(new ScanHanaTableFn(options.getDriver(), options.getConnectionString(),
            options.getUsername(), options.getPassword(), query)) 
      .apply(BigQueryIO.writeTableRows().to(options.getDestDataset() + "." + options.getTableName())
          .withSchema(bqHanaSchema)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
    pipeline.run();
  }
}
