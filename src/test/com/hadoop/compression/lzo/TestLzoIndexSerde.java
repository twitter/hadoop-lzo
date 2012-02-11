package com.hadoop.compression.lzo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class TestLzoIndexSerde extends TestCase {

  public void testBasicSerde() throws IOException {
    testGenericSerde(new LzoBasicIndexSerde());
  }

  /**
   * Ensures that the provided serde can read its own output correctly
   * @param serde
   * @throws IOException
   */
  public void testGenericSerde(LzoIndexSerde serde) throws IOException {
   long[] expected = { 40L, 500L, 584L, 10017L };
   ByteArrayOutputStream baos = new ByteArrayOutputStream();
   DataOutputStream os = new DataOutputStream(baos);
   serde.prepareToWrite(os);
   for (long val : expected) {
     serde.writeOffset(val);
   }
   serde.finishWriting();

   ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
   DataInputStream is = new DataInputStream(bais);
   long firstLong = is.readLong();
   assertTrue(serde.accepts(firstLong));
   serde.prepareToRead(is);
   for (long val : expected) {
     assertTrue(serde.hasNext());
     assertEquals(val, serde.next());
   }

  }
}
