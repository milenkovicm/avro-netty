#Avro Netty ByteBuf Encoder/Decoder [![Build Status](https://api.travis-ci.org/milenkovicm/avro-netty.svg)](https://travis-ci.org/milenkovicm/avro-netty)

Simple [Apache Avro](avro.apache.org) `Encoder/Decoder` implemented using [netty.io](netty.io) byte buffers ( `io.netty.buffer.ByteBuf` ). The main motivation for this implementation is to utilize Netty's powerful `ButeBuf` pooling.

##How to use?

To use `Encoder` just create new instance:

```
// GenericRecord record - record to encode 
final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(1024);
final ByteBufEncoder encoder = new ByteBufEncoder();
encoder.setBuffer(buffer);
final DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());

try {
    writer.write(record, encoder);
    final ByteBuf buffer = encoder.getBuffer();
	// do something with buffer, but don't forget to release it 
} catch (final Exception e) {
    throw new RuntimeException(e);
} 
```

Similar for `Decoder`:

```
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

##Note
 
To get best performance from decoder don't forget to use pooled buffers and turn leak detection off!

```
java -Dio.netty.leakDetectionLevel=DISABLED -Dio.netty.allocator.type=pooled  ...
```

Encoder/Decoder is not thread safe, developer should ensure thread safety if needed.

##To follow

Add handlers for object serialization/deserialization handlers.
 