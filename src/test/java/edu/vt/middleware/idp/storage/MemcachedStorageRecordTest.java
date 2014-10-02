/*
 * See LICENSE for licensing and NOTICE for copyright.
 */

package edu.vt.middleware.idp.storage;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Unit test for {@link MemcachedStorageRecord} class.
 */
public class MemcachedStorageRecordTest {

    @Test
    public void testNumericExpiration() {
        final MemcachedStorageRecord record = new MemcachedStorageRecord("r1", 5031757792L);
        assertEquals(record.getExpiration().longValue(), 5031757792L);
        assertEquals(record.getMemcachedExpiration(), 5031757);
    }

    @Test
    public void testNullExpiration() {
        final MemcachedStorageRecord record = new MemcachedStorageRecord("r2", null);
        assertNull(record.getExpiration());
        assertEquals(record.getMemcachedExpiration(), 0);
    }
}