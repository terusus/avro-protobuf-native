package terusus.avro.protobuf;

import com.google.protobuf.Descriptors.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

public class TestScalar {
    @Test
    public void testScalarSerialization() throws Exception {
        Descriptor descriptor = ProtobufHelper.getDescriptor("src/test/resources/scalar.desc", "scalar");
        Schema schema = ExtendedProtobufData.get().getSchema(descriptor);
        GenericRecord record = new GenericRecordBuilder(schema)
            .set("string", "abc")
            .build();

        DatumWriter<GenericRecord> writer = new ProtobufNativeDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = new ProtobufEncoder(outputStream);

        writer.write(record, encoder);
        encoder.flush();

        byte[] result = outputStream.toByteArray();

        System.out.println("bye");
    }
}
