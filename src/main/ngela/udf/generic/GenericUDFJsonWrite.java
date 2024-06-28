package ngela.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.json.HiveJsonWriter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * Hive struct to json string
 */
@Description(name = "json_write", value = "_FUNC_(value) - "
        + "Change the given complex type to json.", extended = ""
        + "Parsed as null: if the json is null, it is the empty string or if it contains only whitespaces\n"
        + "Example:\n" + "select _FUNC_(NAMED_STRUCT('name', 'haha', 'age', 17)")
public class GenericUDFJsonWrite extends GenericUDF {
    private ObjectInspector sourceInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 1, 1);
        sourceInspector = arguments[0];
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        final Object valObject = arguments[0].get();
        if (valObject == null) {
            return null;
        }
        try {
            final HiveJsonWriter jsonWriter = new HiveJsonWriter();
            String jsonString = jsonWriter.write(valObject, sourceInspector);
            return new Text(jsonString);
        } catch (SerDeException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("json_write", children);
    }
}
