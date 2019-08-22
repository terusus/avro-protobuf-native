package terusus.avro.protobuf

import java.io.OutputStream
import java.nio.ByteBuffer

import com.google.protobuf.CodedOutputStream
import org.apache.avro.io.Encoder
import org.apache.avro.util.Utf8

class ProtobufEncoder(outputStream: OutputStream) extends Encoder {
  private val codedStream = CodedOutputStream.newInstance(outputStream)

  def writeTag(fieldNumber: Int, wireType: Int): Unit = codedStream.writeTag(fieldNumber, wireType)
  def writeRaw(raw: Int): Unit = codedStream.writeUInt32NoTag(raw)

  override def writeNull(): Unit = {} // do nothing
  override def writeBoolean(b: Boolean): Unit = codedStream.writeBoolNoTag(b)
  override def writeInt(n: Int): Unit = codedStream.writeInt32NoTag(n)
  override def writeLong(n: Long): Unit = codedStream.writeInt64NoTag(n)
  override def writeFloat(f: Float): Unit = codedStream.writeFloatNoTag(f)
  override def writeDouble(d: Double): Unit = codedStream.writeDoubleNoTag(d)
  override def writeString(utf8: Utf8): Unit = codedStream.writeStringNoTag(utf8.toString)
  override def writeBytes(bytes: ByteBuffer): Unit = codedStream.writeByteArrayNoTag(bytes.array())
  override def writeBytes(bytes: Array[Byte], start: Int, len: Int): Unit = codedStream.writeByteArrayNoTag(bytes)
  override def writeFixed(bytes: Array[Byte], start: Int, len: Int): Unit = codedStream.writeByteArrayNoTag(bytes)
  override def writeEnum(e: Int): Unit = codedStream.writeEnumNoTag(e)

  override def writeArrayStart(): Unit = {} // arrays are specially managed
  override def setItemCount(itemCount: Long): Unit = {} // arrays are specially managed
  override def startItem(): Unit = {} // arrays are specially managed
  override def writeArrayEnd(): Unit = {} // arrays are specially managed

  override def writeMapStart(): Unit = ???

  override def writeMapEnd(): Unit = ???

  override def writeIndex(unionIndex: Int): Unit = {} // TODO: How to manage unions? (oneof)

  override def flush(): Unit = codedStream.flush()
}
