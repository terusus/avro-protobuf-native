package terusus.avro.protobuf

import java.io.ByteArrayOutputStream
import java.util.{List => JList}

import com.google.protobuf.WireFormat
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.Encoder

class ProtobufNativeDatumWriter[T](rootSchema: Schema) extends GenericDatumWriter[T](rootSchema) {
  override def writeWithoutConversion(schema: Schema, datum: Any, out: Encoder): Unit = {
    val protobufEncoder = out.asInstanceOf[ProtobufEncoder]
    val maybeNumber = Option(schema.getObjectProp(ExtendedProtobufData.KEY_NUMBER)).asInstanceOf[Option[Int]]
    val maybePacked = Option(schema.getObjectProp(ExtendedProtobufData.KEY_PACKED)).asInstanceOf[Option[Boolean]]

    maybePacked -> maybeNumber match {
      case (Some(true), Some(number)) => protobufEncoder.writeTag(number, WireFormat.WIRETYPE_LENGTH_DELIMITED)
      case (None, Some(number)) => protobufEncoder.writeTag(number, schema.getObjectProp(ExtendedProtobufData.KEY_WIRE).asInstanceOf[Int])
      case _ => // do not write a tag
    }

    super.writeWithoutConversion(schema, datum, out)
  }

  override def writeArray(schema: Schema, datum: Any, out: Encoder): Unit = {
    val list = datum.asInstanceOf[JList[Any]]

    if (schema.getObjectProp(ExtendedProtobufData.KEY_PACKED).asInstanceOf[Boolean]) {
      val elementSchema = schema.getElementType
      val newStream = new ByteArrayOutputStream()
      val newProtobufEncoder = new ProtobufEncoder(newStream)

      list.forEach(element => super.writeWithoutConversion(elementSchema, element, newProtobufEncoder))

      newProtobufEncoder.flush()
      newStream.close()

      out.writeBytes(newStream.toByteArray, 0, newStream.size)
    }
    else {
      // overridden method is used here in order to write the tag
      list.forEach(element => writeWithoutConversion(schema.getElementType, element, out))
    }
  }

  override def writeRecord(schema: Schema, datum: Any, out: Encoder): Unit = {
    if (rootSchema == schema) {
      super.writeRecord(schema, datum, out)
    }
    else {
      val newStream = new ByteArrayOutputStream()
      val newProtobufEncoder = new ProtobufEncoder(newStream)

      super.writeRecord(schema, datum, newProtobufEncoder)

      newProtobufEncoder.flush()
      newStream.close()

      out.writeBytes(newStream.toByteArray, 0, newStream.size)
    }
  }
}
