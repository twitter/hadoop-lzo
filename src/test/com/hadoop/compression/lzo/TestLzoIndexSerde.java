package com.hadoop.compression.lzo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class TestLzoIndexSerde extends TestCase {

  public void testBasicSerde() throws IOException, InstantiationException, IllegalAccessException {
    testGenericSerde(new LzoBasicIndexSerde());
  }

  public void testLzoTinyOffsetsSerde() throws IOException, InstantiationException, IllegalAccessException {
    testGenericSerde(new LzoTinyOffsetsSerde());
  }

  /**
   * Ensures that the provided serde can read its own output correctly
   * @param serde
   * @throws IOException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public void testGenericSerde(LzoIndexSerde serde) throws IOException, InstantiationException, IllegalAccessException {
   long[] expected = { 40L, 500L, 584L, 10017L };
   ByteArrayOutputStream baos = new ByteArrayOutputStream();
   DataOutputStream os = new DataOutputStream(baos);
   serde.prepareToWrite(os);
   for (long val : expected) {
     serde.writeOffset(val);
   }
   serde.finishWriting();
   serde = serde.getClass().newInstance();

   ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
   DataInputStream is = new DataInputStream(bais);
   long firstLong = is.readLong();
   assertTrue("Serde does not accept its own first long", serde.accepts(firstLong));
   serde.prepareToRead(is);
   assertEquals("Serde reports different number of blocks than expected", expected.length, serde.numBlocks());
   for (long val : expected) {
     assertTrue("Serde does not return as many values as were written", serde.hasNext());
     assertEquals("Serde returned wrong offset", val, serde.next());
   }
   assertFalse(serde.hasNext());

  }
}
