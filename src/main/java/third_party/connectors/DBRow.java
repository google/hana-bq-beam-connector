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

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;

/*
 * Class to hold the values that represent a row in a Database.
 */
@AutoValue
@DefaultCoder(SerializableCoder.class)
public abstract class DBRow implements Serializable{
  /**
   * Manually create a DB row.
   */
  public static DBRow create(List<Object> fields) {
    return new AutoValue_DBRow(fields);
  }

  public abstract List<Object> fields();
}



