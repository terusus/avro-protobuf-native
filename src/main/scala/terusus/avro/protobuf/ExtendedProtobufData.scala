package terusus.avro.protobuf

import java.io.{File, IOException}
import java.util.{ArrayList => JArrayList, Arrays, IdentityHashMap => JIdentityHashMap, Map => JMap}

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.protobuf.Descriptors
import org.apache.avro.Schema
import org.apache.avro.protobuf.ProtobufData
import org.apache.avro.util.internal.Accessor
import com.google.protobuf.Descriptors.FieldDescriptor.Type._
import org.apache.avro.generic.GenericData

// This is a super dirty copy and paste of the official code.
// If my PR gets accepted the getNamespace, getSchema(FieldDescriptor), getDefault(FieldDescriptor) methods should become public.
// This will allow to easily extend ProtobufData and add the needed metadata from the field descriptor.
//
// https://github.com/apache/avro/pull/612
object ExtendedProtobufData extends ProtobufData {
  val KEY_NUMBER = "number"
  val KEY_TYPE = "type"
  val KEY_WIRE = "wire"
  val KEY_PACKED = "packed"

  private def getSchema(f: Descriptors.FieldDescriptor) = {
    var s: Schema = getNonRepeatedSchema(f)
    s.addProp(KEY_NUMBER, f.getNumber)
    s.addProp(KEY_WIRE, f.getLiteType.getWireType)

    if (f.isRepeated) {
      s = Schema.createArray(s)
      s.addProp(KEY_NUMBER, f.getNumber)
      s.addProp(KEY_WIRE, f.getLiteType.getWireType)
      s.addProp(KEY_PACKED, f.isPacked)
    }

    s
  }

  // custom code ends here

  private val SEEN: ThreadLocal[JMap[Descriptors.Descriptor, Schema]] = ThreadLocal.withInitial(() => new JIdentityHashMap())
  private val NULL = Schema.create(Schema.Type.NULL)
  private val FACTORY = new JsonFactory
  private val MAPPER = new ObjectMapper(FACTORY)
  private val NODES = JsonNodeFactory.instance

  private def getNonRepeatedSchema(f: Descriptors.FieldDescriptor): Schema = {
    var result: Schema = null
    f.getType match {
      case BOOL =>
        Schema.create(Schema.Type.BOOLEAN)
      case FLOAT =>
        Schema.create(Schema.Type.FLOAT)
      case DOUBLE =>
        Schema.create(Schema.Type.DOUBLE)
      case STRING =>
        val s = Schema.create(Schema.Type.STRING)
        GenericData.setStringType(s, GenericData.StringType.String)
        s
      case BYTES =>
        Schema.create(Schema.Type.BYTES)
      case INT32 => Schema.create(Schema.Type.INT)
      case UINT32 => Schema.create(Schema.Type.INT)
      case SINT32 => Schema.create(Schema.Type.INT)
      case FIXED32 => Schema.create(Schema.Type.INT)
      case SFIXED32 => Schema.create(Schema.Type.INT)
      case INT64 => Schema.create(Schema.Type.LONG)
      case UINT64 => Schema.create(Schema.Type.LONG)
      case SINT64 => Schema.create(Schema.Type.LONG)
      case FIXED64 => Schema.create(Schema.Type.LONG)
      case SFIXED64 => Schema.create(Schema.Type.LONG)
      case ENUM =>
        getSchema(f.getEnumType)
      case MESSAGE =>
        result = getSchema(f.getMessageType)
        if (f.isOptional) { // wrap optional record fields in a union with null
          result = Schema.createUnion(Arrays.asList(NULL, result))
        }
        result

      case _ =>
        throw new RuntimeException("Unexpected type: " + f.getType)
    }
  }

  override def getSchema(descriptor: Descriptors.Descriptor): Schema = {
    val seen = SEEN.get
    if (seen.containsKey(descriptor)) { // stop recursion
      return seen.get(descriptor)
    }
    val first = seen.isEmpty
    try {
      val result = Schema.createRecord(descriptor.getName, null, getNamespace(descriptor.getFile, descriptor.getContainingType), false)
      seen.put(descriptor, result)
      val fields = new JArrayList[Schema.Field]
      import scala.collection.JavaConversions._
      for (f <- descriptor.getFields) {
        fields.add(Accessor.createField(f.getName, getSchema(f), null, getDefault(f)))
      }
      result.setFields(fields)
      result
    } finally if (first) seen.clear()
  }

  private def getDefault(f: Descriptors.FieldDescriptor): JsonNode = {
    if (f.isRequired || f.isRepeated) { // no default
      return null
    }
    if (f.hasDefaultValue) { // parse spec'd default value
      var value = f.getDefaultValue
      f.getType match {
        case ENUM =>
          value = value.asInstanceOf[Descriptors.EnumValueDescriptor].getName
      }
      val json = toString(value)
      try return MAPPER.readTree(FACTORY.createParser(json))
      catch {
        case e: IOException =>
          throw new RuntimeException(e)
      }
    }
    f.getType match { // generate default for type
      case BOOL =>
        NODES.booleanNode(false)
      case FLOAT => NODES.numberNode(0)
      case DOUBLE => NODES.numberNode(0)
      case INT32 => NODES.numberNode(0)
      case UINT32 => NODES.numberNode(0)
      case SINT32 => NODES.numberNode(0)
      case FIXED32 => NODES.numberNode(0)
      case SFIXED32 => NODES.numberNode(0)
      case INT64 => NODES.numberNode(0)
      case UINT64 => NODES.numberNode(0)
      case SINT64 => NODES.numberNode(0)
      case FIXED64 => NODES.numberNode(0)
      case SFIXED64 => NODES.numberNode(0)
      case STRING => NODES.textNode("")
      case BYTES => NODES.textNode("")
      case ENUM => NODES.textNode(f.getEnumType.getValues.get(0).getName)
      case MESSAGE => NODES.nullNode

      case _ =>
        throw new RuntimeException("Unexpected type: " + f.getType)
    }
  }

  private def getNamespace(fd: Descriptors.FileDescriptor, descriptor: Descriptors.Descriptor) = {
    var containing = descriptor
    val o = fd.getOptions
    val p = if (o.hasJavaPackage) o.getJavaPackage
    else fd.getPackage
    var outer = ""
    if (!o.getJavaMultipleFiles) if (o.hasJavaOuterClassname) outer = o.getJavaOuterClassname
    else {
      outer = new File(fd.getName).getName
      outer = outer.substring(0, outer.lastIndexOf('.'))
      outer = toCamelCase(outer)
    }
    val inner = new StringBuilder
    while ( {
      containing != null
    }) {
      if (inner.length == 0) inner.insert(0, containing.getName)
      else inner.insert(0, containing.getName + "$")
      containing = containing.getContainingType
    }
    val d1 = if (!(outer.isEmpty) || inner.length != 0) "."
    else ""
    val d2 = if (!(outer.isEmpty) && inner.length != 0) "$"
    else ""
    p + d1 + outer + d2 + inner
  }

  private def toCamelCase(s: String) = {
    val parts = s.split("_")
    val camelCaseString = new StringBuilder
    for (part <- parts) {
      camelCaseString.append(cap(part))
    }
    camelCaseString.toString
  }

  private def cap(s: String) = s.substring(0, 1).toUpperCase + s.substring(1).toLowerCase
}
