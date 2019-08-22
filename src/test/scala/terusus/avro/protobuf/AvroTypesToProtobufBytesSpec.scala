package terusus.avro.protobuf

import java.io.ByteArrayOutputStream

import com.google.protobuf.DynamicMessage
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}

import scala.collection.JavaConverters._

class AvroTypesToProtobufBytesSpec extends BaseSpec {
  "Avro to Protobuf" should {
    val descriptorUri = UriHelper.create("src/test/resources/complextopic.desc")
    val messageDescriptor = ProtobufHelper.getDescriptorByName(descriptorUri, "root").get
    val schema = ExtendedProtobufData.getSchema(messageDescriptor)
    val innerSchema = schema.getField("rootInner").schema().getTypes.get(1)

    val record = new GenericRecordBuilder(schema)
      .set("rootString", "theRootString")
      .set("rootInt", 7L)
      .set("rootInner", new GenericRecordBuilder(innerSchema)
        .set("innerString", "theInnerString")
        .set("innerInt", 8L)
        .set("innerRepeatedInt", List(9L, 10L).asJava)
        .set("innerRepeatedString", List("first", "second").asJava)
        .build()
      )
      .build()

    val writer = new ProtobufNativeDatumWriter[GenericRecord](schema)
    val outputStream = new ByteArrayOutputStream
    val encoder = new ProtobufEncoder(outputStream)

    writer.write(record, encoder)

    encoder.flush()

    val maybeResult = ProtobufHelper.convertProtobufToValues(outputStream.toByteArray, messageDescriptor)

    println("the end")
  }
}
