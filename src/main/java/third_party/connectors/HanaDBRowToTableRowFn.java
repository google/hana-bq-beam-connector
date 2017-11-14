package third_party.connectors;

import com.google.api.services.bigquery.model.TableRow;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import third_party.connectors.DBRow;

/*
 * DoFn to take a {@link DBRow} and convert to a TableRow for use with
 * BigQuery. using the column names provided via side input. 
 */
public class HanaDBRowToTableRowFn extends DoFn<DBRow, TableRow> {
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
