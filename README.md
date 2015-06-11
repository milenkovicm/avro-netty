# Avro Netty ByteBuf Encoder/Decoder [![Build Status](https://api.travis-ci.org/milenkovicm/avro-netty.svg)](https://travis-ci.org/milenkovicm/avro-netty)

Simple [Apache Avro](avro.apache.org) `Encoder/Decoder` implemented using [netty.io](netty.io) byte buffers ( `io.netty.buffer.ByteBuf` ). The main motivation for this implementation is to utilize Netty's powerful `ButeBuf` pooling.

## How to use?

To use `Encoder` just create new instance:

```java
// GenericRecord record - record to encode 
final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(1024);
final ByteBufEncoder encoder = new ByteBufEncoder();
encoder.setBuffer(buffer); // buffer to be used to store serialized object 
final DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());

try {
    writer.write(record, encoder);
    final ByteBuf result = encoder.getBuffer(); // return the buffer with the serialized object
	// do something with result, but don't forget to release it 
} catch (final Exception e) {
    throw new RuntimeException(e);
} 
```

`Decoder` has similar api:

```java
// ByteBuf buffer - buffer to decode
final ByteBufDecoder decoder = new ByteBufDecoder();
decoder.setBuffer(buffer);
final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(record.getSchema());
try {
    final GenericRecord record = reader.read(null, decoder);
    // do something with record
} catch (final IOException e) {
    throw new RuntimeException(e);
}
```

### Encoder/Decoder Note
 
To get best performance from decoder don't forget to use pooled buffers and turn leak detection off!

```bash
$java -Dio.netty.leakDetectionLevel=DISABLED -Dio.netty.allocator.type=pooled  ...
```

Encoder/Decoder is not thread safe, developer should ensure thread safety if needed.

## NettyGenericDatumWriter

Default `DatumWriter` implementation `GenericDatumWriter` expects `java.nio.ByteBuffer` for byte array properties, which may not be optimal for all use cases especially if powerful netty pooled buffer allocator is used. `NettyGenericDatumWriter` has ability to write `io.netty.buffer.ByteBuf` if used for byte array properties. 

```java
byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
final GenericRecord record = new GenericData.Record(E_BYTES.SCHEMA$);
// use ByteBuf for byte array propertie
record.put("f_value", ByteBufAllocator.DEFAULT.buffer(value.length).writeBytes(value));

final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(64);
final ByteBufEncoder encoder = new ByteBufEncoder();
encoder.setBuffer(buffer);

final DatumWriter<GenericRecord> writer = new NettyGenericDatumWriter<GenericRecord>(record.getSchema());
writer.write(record, encoder);
```

Note that if you use `ByteBuf` as a generic record property you'll have to release it manually, `NettyGenericDatumWriter` wont release it for you.

schema used:

```
{"namespace": "com.github.milenkovicm.avro.test.event",
 "type": "record",
 "name": "E_BYTES",     
  "fields": [
 
      {"name": "f_value",   "type": "bytes"}
      ]
}
```

## NettyGenericDatumReader

Default `DatumReader` implementation `GenericDatumReader` uses `java.nio.ByteBuffer` for byte array properties, which may not be optimal for all use cases especially if powerful netty pooled buffer allocator is used. `NettyGenericDatumReader` has ability to read byte arrays as `io.netty.buffer.ByteBuf`. Be careful as `ByteBuf` properties should be released when not needed.

```java
final ByteBufDecoder decoder = new ByteBufDecoder();
decoder.setBuffer(buffer);
final GenericDatumReader<GenericRecord> reader = new NettyGenericDatumReader<GenericRecord>(schema);
GenericRecord record =  reader.read(null, decoder);
```

## Don't forget!

Don't forget two things:

 1. to release `ByteBuf` when not needed anymore.
 2. implementations are not thread safe.
 
that's it!
