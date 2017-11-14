package third_party.connectors;

import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;


/*
 * PTransform that connects to a SAP Hana DB and reads data from the query and puts
 * the resulting rows into DBRow PCollection. The columns are ordered in the same 
 * order as the provided column names list. 
 *
 * The incoming PCollection<String> is a list of comma seperated intervals to use
 * when chunking the table. 
 */
public class HanaToDBRow extends PTransform<PCollection<String>, PCollection<DBRow>> {
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

    /*
     * Use JdbcIO to read from Hana and map each row to a DBRow object.
     */
    @Override
    public PCollection<DBRow> expand(PCollection<String> input) {
      return input.apply("Hana JDBC IO", JdbcIO.<String, DBRow>readAll()
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
          })
        .withParameterSetter(new JdbcIO.PreparedStatementSetter<String>() {
          @Override
          public void setParameters(String element, PreparedStatement statement)
            throws Exception {
            String[] startAndStop = element.split(",");
            statement.setString(1, startAndStop[0]);
            statement.setString(2, startAndStop[1]);
          }
        }));
    }
  }

