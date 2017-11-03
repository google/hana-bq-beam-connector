package connectors;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;


@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class DBRow implements Serializable{
  /**
   * Manually create a test row.
   */
  public static DBRow create(List<Object> fields, List<String> col_names, List<Integer> col_types) {
    return new AutoValue_DBRow(fields, col_names, col_types);
  }

  public abstract List<Object> fields();
  public abstract List<String> col_names();
  public abstract List<Integer> col_types();
}



