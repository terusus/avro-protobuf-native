package terusus.avro.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;

public class ExtendedProtobufData extends ProtobufData {
    final private static ExtendedProtobufData instance;

    final public static String KEY_NUMBER = "number";
    final public static String KEY_WIRE = "wire";
    final public static String KEY_PACKED = "packed";

    static {
        instance = new ExtendedProtobufData();
    }

    public static ExtendedProtobufData get() {
        return instance;
    }

    @Override
    public Schema getSchema(FieldDescriptor fieldDescriptor) {
        Schema schema = super.getSchema(fieldDescriptor);

        schema.addProp(KEY_NUMBER, fieldDescriptor.getNumber());
        schema.addProp(KEY_WIRE, fieldDescriptor.getLiteType().getWireType());

        if (fieldDescriptor.isRepeated()) {
            Schema elementSchema = schema.getElementType();

            schema.addProp(KEY_PACKED, fieldDescriptor.isPacked());
            elementSchema.addProp(KEY_NUMBER, fieldDescriptor.getNumber());
            elementSchema.addProp(KEY_WIRE, fieldDescriptor.getLiteType().getWireType());
        }

        return schema;
    }
}
