package terusus.avro.protobuf;

import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

public class ProtobufHelper {
    public static Descriptor getDescriptor(String descriptorFilename, String messageName) throws Exception {
        Descriptor result = null;
        File descriptorFile = new File(descriptorFilename);

        try (InputStream inputStream = new FileInputStream(descriptorFile)) {
            FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(inputStream);
            List<FileDescriptorProto> fileDescriptorProtoList = fileDescriptorSet.getFileList();
            for (FileDescriptorProto fileDescriptorProto : fileDescriptorProtoList) {
                result = FileDescriptor
                    .buildFrom(fileDescriptorProto, new FileDescriptor[0])
                    .findMessageTypeByName(messageName);
            }
        }

        return result;
    }
}
