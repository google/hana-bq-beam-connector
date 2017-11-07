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

package third_party.connectors;

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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
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
  private static String schemaQueryTemplate = "SELECT COLUMN_NAME,DATA_TYPE_NAME FROM TABLE_COLUMNS WHERE TABLE_NAME = '%s' ORDER BY POSITION ASC";
  private static String queryTemplate = "SELECT * FROM %s WHERE %s >= %s %s";
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

  public static class HanaToDBRow extends PTransform<PBegin, PCollection<DBRow>> {

    private final String driver;
    private final String connectionString;
    private final String username;
    private final String password;
    private final String query;
    private final List<String> columnNames;

    private HanaToDBRow(Reader reader) {
      this.driver = reader.driver;
      this.connectionString = reader.connectionString;
      this.username = reader.username;
      this.password = reader.password;
      this.query = reader.query;
      this.columnNames = reader.columnNames;
    }

    public static class Reader {
      private final String driver;
      private final String connectionString;
      private String username;
      private String password;
      private String query;
      private List<String> columnNames;

      public Reader(String driver, String connectionString) {
        this.driver = driver;
        this.connectionString = connectionString;
      }

      public Reader withUsername(String username) {
        this.username = username;
        return this;
      }

      public Reader withPassword(String password) {
        this.password = password;
        return this;
      }

      public Reader withQuery(String query) {
        this.query = query;
        return this;
      }

      public Reader withColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
        return this;
      }

      public HanaToDBRow create() {
        return new HanaToDBRow(this);
      }
    }

    @Override
    public PCollection<DBRow> expand(PBegin input) {
      Pipeline pipeline = input.getPipeline();

      return pipeline.apply("Hana JDBC IO", JdbcIO.<DBRow>read()
          .withCoder(SerializableCoder.of(DBRow.class))
          .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
              this.driver, this.connectionString)
            .withUsername(this.username)
            .withPassword(this.password))

          .withQuery(this.query)
          .withRowMapper(new JdbcIO.RowMapper<DBRow>() {
            public DBRow mapRow(ResultSet resultSet) throws Exception {
              ResultSetMetaData meta = resultSet.getMetaData();
              int columnCount = meta.getColumnCount();
              List<Object> values = new ArrayList<Object>();
              for (int column = 1; column <= columnCount; ++column)
              {
                String name = HanaToDBRow.this.columnNames.get(column - 1);
                Object value = resultSet.getObject(name);
                values.add(value);

              }
              return DBRow.create(values);
            }
          }));
    }
  }

  public static class HanaDBRowToTableRowFn extends DoFn<DBRow, TableRow> {
    PCollectionView<List<String>> columnNamesCollection;

    public HanaDBRowToTableRowFn(PCollectionView<List<String>> columnNamesCollection) {
      this.columnNamesCollection = columnNamesCollection;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DBRow data = c.element();
      List<String> columnNames = c.sideInput(columnNamesCollection);
      List<Object> fields = data.fields();
      TableRow row = new TableRow();
      for(int i = 0; i < fields.size(); i++)
      {
        Object fieldData = fields.get(i);
        String columnName = columnNames.get(i);
        if(fieldData == null)
          continue;
        String sFieldData = fieldData.toString();
        if(!sFieldData.toLowerCase().equals("null"))
          row.put(columnName, sFieldData);
      }
      c.output(row);
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

        ResultSet resultSet = stmt.executeQuery(String.format(schemaQueryTemplate, options.getTableName()));
        Map<String, String> columns = new HashMap<String,String>();
        List<String> orderedColumns = new ArrayList<String>();
        while(resultSet.next())
        {
          String columnName = resultSet.getString("COLUMN_NAME");
          String type = resultSet.getString("DATA_TYPE_NAME");
          columns.put(columnName, type);
          orderedColumns.add(columnName);
        }
        TableSchema schema = new TableSchema();
        List<TableFieldSchema> tableFieldSchema = new ArrayList<TableFieldSchema>();
        for(String columnName : orderedColumns)
        {
          TableFieldSchema schemaEntry = new TableFieldSchema();
          schemaEntry.setName(columnName);
          String type = columns.get(columnName);
          if(hanaToBqTypeMap.containsKey(type))
            schemaEntry.setType(hanaToBqTypeMap.get(type));
          else {
            logger.severe("Unhandled Hana type: " +  type);
            throw new SQLException("Unhandled Hana Type");
          }
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
    List<String> columnNames = new ArrayList<String>();
    for(TableFieldSchema fieldSchema : bqHanaSchema.getFields())
    {
      columnNames.add(fieldSchema.getName());
    }

    String queryClause = "";
    if(options.getEndTime() != null)
    {
      queryClause += " AND " + options.getTimestampColumn() + " < " + options.getEndTime();
    }
    String query = String.format(queryTemplate, options.getTableName(), options.getTimestampColumn(),
        options.getStartTime(), queryClause);

    PCollectionView<List<String>> columnNamesCollection = pipeline
      .apply(Create.of(columnNames)).setCoder(StringUtf8Coder.of())
      .apply(View.<String>asList());
    logger.info(query);
    pipeline
      .apply(new HanaToDBRow.Reader(options.getDriver(), options.getConnectionString())
         .withUsername(options.getUsername())
         .withPassword(options.getPassword())
         .withQuery(query)
         .withColumnNames(columnNames)
         .create())
      .apply("Hana row to BQ TableRow",ParDo.of(new HanaDBRowToTableRowFn(columnNamesCollection))
          .withSideInputs(columnNamesCollection))
      .apply(BigQueryIO.writeTableRows().to(options.getDestDataset() + "." + options.getTableName())
          .withSchema(bqHanaSchema)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
    pipeline.run();
  }
}
