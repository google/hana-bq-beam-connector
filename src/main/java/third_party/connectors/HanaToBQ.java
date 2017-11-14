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
import com.google.common.collect.ImmutableMap;
import java.security.SecureRandom;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import third_party.connectors.DBRow;
import third_party.connectors.HanaDBRowToTableRowFn;
import third_party.connectors.HanaToDBRow;


public class HanaToBQ {
  private static Logger logger = Logger.getLogger(HanaToBQ.class.getSimpleName());
  private static String schemaQueryTemplate = "SELECT COLUMN_NAME,DATA_TYPE_NAME FROM TABLE_COLUMNS WHERE TABLE_NAME = '%s' ORDER BY POSITION ASC";
  private static String chunkQueryTemplate = "SELECT t.%1$s FROM (SELECT %1$s, ROW_NUMBER() OVER (ORDER BY %1$s) AS rownum FROM %2$S WHERE %1$s > %3$s) AS t WHERE MOD(rownum, %4$s) = 0 and t.%1$s > %3$s ORDER BY t.%1$s";
  private static String queryTemplate = "SELECT * FROM %1$s WHERE %2$s >= ? AND %2$s < ?";
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

  /**
   * Helper function to retrieve a connection object for Hana.
   * @param options Options class with connection information.
   * @return Connection object to use for working with Hana DB.
   */
  public static Connection getConnection(Options options) {
    Connection connection = null;
    try {
      connection = DriverManager.getConnection(options.getConnectionString(), options.getUsername(), options.getPassword());
    } catch (SQLException e) {
      System.err.println("Connection Failed: " + e.getMessage());
    }
    return connection;
  }


  /**
   * Retrieves the schema from a hana database table and converts it to BigQuery TableSchema object.
   * @param table table name in the database.
   * @return TableSchema for the table
   */
  public static TableSchema getSchema(Options options) {
    Connection connection = getConnection(options);
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
        logger.severe("Query failed while getting schema: " + e.getMessage());
      }
    }
    return null;
  }

  /**
   * Method to get the timestamps that correspond to the chunking specified by the user.
   * @param options Options class with chunking and table information
   * @return List<String> of timestamp pairs, comma separated, indicating time chunks.
   */
  public static List<String> getChunkIntervals(Options options) {
    Connection connection = getConnection(options);
    List<String> results = new ArrayList<String>();
    String prevStart = options.getStartTime();
    if (connection != null) {
      try {
        Statement stmt = connection.createStatement();
        String query = String.format(chunkQueryTemplate, options.getTimestampColumn(),
            options.getTableName(), options.getStartTime(), options.getChunkSize().toString());
        logger.info(query);
        ResultSet resultSet = stmt.executeQuery(query);
        while(resultSet.next()) {
          String intervals = prevStart + "," + resultSet.getString(1); 
          results.add(intervals);
          prevStart = resultSet.getString(1);
        }
        results.add(prevStart + "," + "2000000000");
      } catch(SQLException e) {
        logger.severe("Query for chunk size failed: " + e.getMessage());
      }
    }
    return results;
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

    @Description("Approximate size of chunks to read")
    @Default.Integer(1000000)
    Integer getChunkSize();
    void setChunkSize(Integer value);
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

    String query = String.format(queryTemplate, options.getTableName(), options.getTimestampColumn());

    List<String> chunkSpots = getChunkIntervals(options);
    logger.info(chunkSpots.toString());
    PCollectionView<List<String>> columnNamesCollection = pipeline
      .apply(Create.of(columnNames)).setCoder(StringUtf8Coder.of())
      .apply(View.<String>asList());
    logger.info(query);

    pipeline
      .apply(Create.of(chunkSpots)).setCoder(StringUtf8Coder.of())
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
