package terusus.avro.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.google.protobuf.WireFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

public class ProtobufNativeDatumWriter<D> extends GenericDatumWriter<D> {
    final private Schema rootSchema;

    public ProtobufNativeDatumWriter(Schema rootSchema) {
        super(rootSchema);

        this.rootSchema = rootSchema;
    }

    @Override
    public void writeWithoutConversion(Schema schema, Object datum, Encoder out) throws IOException {
        ProtobufEncoder protobufEncoder = (ProtobufEncoder)out;
        boolean isPacked = (Boolean)schema.getObjectProp(ExtendedProtobufData.KEY_PACKED);
        Optional<Integer> maybeNumber = Optional.ofNullable((Integer)schema.getObjectProp(ExtendedProtobufData.KEY_NUMBER));

        if (maybeNumber.isPresent()) {
            int number = maybeNumber.get();

            if (isPacked) {
                protobufEncoder.writeTag(number, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            } else {
                int wire = (Integer)schema.getObjectProp(ExtendedProtobufData.KEY_WIRE);

                protobufEncoder.writeTag(number, wire);
            }
        }

        super.writeWithoutConversion(schema, datum, out);
    }

    @Override
    public void writeArray(Schema schema, Object datum, Encoder out) throws IOException {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>)datum;
        boolean isPacked = (Boolean)schema.getObjectProp(ExtendedProtobufData.KEY_PACKED);

        if (isPacked) {
            Schema elementSchema = schema.getElementType();
            ByteArrayOutputStream newStream = new ByteArrayOutputStream();
            ProtobufEncoder newProtobufEncoder = new ProtobufEncoder(newStream);

            for (Object element : list) {
                super.writeWithoutConversion(elementSchema, element, newProtobufEncoder);
            }

            newProtobufEncoder.flush();
            newStream.close();

            out.writeBytes(newStream.toByteArray(), 0, newStream.size());
        }
        else {
            // overridden method is used here in order to write the tag
            for (Object element : list) {
                writeWithoutConversion(schema.getElementType(), element, out);
            }
        }
    }

    @Override
    public void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
        if (rootSchema == schema) {
            super.writeRecord(schema, datum, out);
        }
        else {
            ByteArrayOutputStream newStream = new ByteArrayOutputStream();
            ProtobufEncoder newProtobufEncoder = new ProtobufEncoder(newStream);

            super.writeRecord(schema, datum, newProtobufEncoder);

            newProtobufEncoder.flush();
            newStream.close();

            out.writeBytes(newStream.toByteArray(), 0, newStream.size());
        }
    }
}
