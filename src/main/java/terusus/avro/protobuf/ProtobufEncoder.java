package terusus.avro.protobuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.google.protobuf.CodedOutputStream;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class ProtobufEncoder extends Encoder {
    final private CodedOutputStream codedStream;

    public ProtobufEncoder(OutputStream outputStream) {
        this.codedStream = CodedOutputStream.newInstance(outputStream);
    }

    public void writeTag(int fieldNumber, int wireType) throws IOException {
        codedStream.writeTag(fieldNumber, wireType);
    }
    public void writeRaw(int raw) throws IOException {
        codedStream.writeUInt32NoTag(raw);
    }

    @Override
    public void writeNull() {} // do nothing
    @Override
    public void writeBoolean(boolean b) throws IOException {
        codedStream.writeBoolNoTag(b);
    }
    @Override
    public void writeInt(int n) throws IOException {
        codedStream.writeInt32NoTag(n);
    }
    @Override
    public void writeLong(long n) throws IOException {
        codedStream.writeInt64NoTag(n);
    }
    @Override
    public void writeFloat(float f) throws IOException {
        codedStream.writeFloatNoTag(f);
    }
    @Override
    public void writeDouble(double d) throws IOException {
        codedStream.writeDoubleNoTag(d);
    }
    @Override
    public void writeString(Utf8 utf8) throws IOException {
        codedStream.writeStringNoTag(utf8.toString());
    }
    @Override
    public void writeBytes(ByteBuffer bytes) throws IOException {
        codedStream.writeByteArrayNoTag(bytes.array());
    }
    @Override
    public void writeBytes(byte[] bytes, int start, int len) throws IOException {
        codedStream.writeByteArrayNoTag(bytes);
    }
    @Override
    public void writeFixed(byte[] bytes, int start, int len) throws IOException {
        codedStream.writeByteArrayNoTag(bytes);
    }
    @Override
    public void writeEnum(int e) throws IOException {
        codedStream.writeEnumNoTag(e);
    }

    @Override
    public void writeArrayStart() {
        // arrays are specially managed;
    }
    @Override
    public void setItemCount(long itemCount) {
        // arrays are specially managed;
    }
    @Override
    public void startItem() {
        // arrays are specially managed;
    }
    @Override
    public void writeArrayEnd() {
        // arrays are specially managed;
    }

    @Override
    public void writeMapStart() {
        // TODO: Implement.
    }

    @Override
    public void writeMapEnd() {
        // TODO: Implement.
    }

    @Override
    public void writeIndex(int unionIndex) {
        // TODO: How to manage unions (oneof)?
    }

    @Override
    public void flush() throws IOException {
        codedStream.flush();
    }
}
