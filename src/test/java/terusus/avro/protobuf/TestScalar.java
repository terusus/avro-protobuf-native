package terusus.avro.protobuf;

import com.google.protobuf.Descriptors.*;
import org.junit.jupiter.api.Test;

public class TestScalar {
    @Test
    public void testScalarSerialization() throws Exception {
        Descriptor descriptor = ProtobufHelper.getDescriptor("src/test/resources/scalar.desc", "scalar");
        System.out.println("bye");
    }
}
