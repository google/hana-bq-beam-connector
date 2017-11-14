package third_party.connectors;

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
  public static DBRow create(List<Object> fields) {
    return new AutoValue_DBRow(fields);
  }

  public abstract List<Object> fields();
}



