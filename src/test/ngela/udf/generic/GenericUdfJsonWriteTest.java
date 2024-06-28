package ngela.udf.generic;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import java.util.List;


import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenericUdfJsonWriteTest {
    @Test
    public void testSimpleStruct() throws Exception {
        try (GenericUDFJsonWrite udf = new GenericUDFJsonWrite()) {
            ObjectInspector[] arguments = {
                    ObjectInspectorFactory.getStandardStructObjectInspector(asList("name", "age"),
                            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                                    PrimitiveObjectInspectorFactory.writableIntObjectInspector))
            };
            udf.initialize(arguments);

            // {"name": "haha", "age", 10} --> 문자열 {"name": "haha", "age", 20}
            List<?> input = asList(new Text("haha"), new IntWritable(10));
            Object res = udf.evaluate(new GenericUDF.DeferredObject[]{
                    new GenericUDF.DeferredJavaObject(input)
            });

            assertTrue(res instanceof Text);
            assertEquals(new Text("{\"name\":\"haha\",\"age\":10}"), res);
        }
    }
}
