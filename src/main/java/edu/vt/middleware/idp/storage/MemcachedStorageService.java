/*
 * See LICENSE for licensing and NOTICE for copyright.
 */

package edu.vt.middleware.idp.storage;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.shibboleth.utilities.java.support.annotation.constraint.NotEmpty;
import net.shibboleth.utilities.java.support.annotation.constraint.Positive;
import net.shibboleth.utilities.java.support.collection.Pair;
import net.shibboleth.utilities.java.support.component.AbstractIdentifiedInitializableComponent;
import net.shibboleth.utilities.java.support.logic.Constraint;
import net.shibboleth.utilities.java.support.primitive.StringSupport;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;
import org.cryptacular.util.ByteUtil;
import org.cryptacular.util.CodecUtil;
import org.cryptacular.util.HashUtil;
import org.opensaml.storage.StorageCapabilities;
import org.opensaml.storage.StorageRecord;
import org.opensaml.storage.StorageSerializer;
import org.opensaml.storage.StorageService;
import org.opensaml.storage.VersionMismatchException;
import org.opensaml.storage.annotation.AnnotationSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memcached storage service. The implementation of context names is based on the implementation of
 * <em>simulated namespaces</em> discussed on the Memcached project site:
 * <p>
 * <a href="https://code.google.com/p/memcached/wiki/NewProgrammingTricks#Namespacing">https://code.google.com/p/memcached/wiki/NewProgrammingTricks#Namespacing</a>
 * <p>
 * This storage service supports arbitrary-length context names and keys despite the limitation of 250-byte length
 * limit on memcached keys. Keys whose length is greater than 250 bytes are hashed using the SHA-512 algorithm and
 * hex encoded to produce a 128-character key that is stored in memcached. Collisions are avoided irrespective of
 * hashing by using the memcached add operation on all create operations which guarantees that an entry is created
 * if and only if a key of the same value does not already exist. Note that context names and keys are assumed to be
 * in the ASCII character set such that key lengths are equal to their size in bytes.
 *
 * @author Marvin S. Addison
 */
public class MemcachedStorageService extends AbstractIdentifiedInitializableComponent implements StorageService {

    /** Maximum length in bytes of memcached keys. */
    private static final int MAX_KEY_LENGTH = 250;

    /** Logger instance. */
    private final Logger logger = LoggerFactory.getLogger(MemcachedStorageService.class);

    /** Handles conversion of {@link MemcachedStorageRecord} to bytes and vice versa. */
    private final Transcoder<MemcachedStorageRecord> storageRecordTranscoder = new StorageRecordTranscoder();

    /** Handles conversion of strings to bytes and vice versa. */
    private final Transcoder<String> stringTranscoder = new StringTranscoder();

    /** Invariant storage capabilities. */
    @Nonnull
    private MemcachedStorageCapabilities capabilities = new MemcachedStorageCapabilities();

    /** Memcached client instance. */
    @Nonnull
    private final MemcachedClient client;

    /** Memcached asynchronous operation timeout in seconds. */
    @Positive
    private int timeout;

    /**
     * Creates a new instance.
     *
     * @param client Memcached client object.
     * @param timeout Memcached operation timeout in seconds.
     */
    public MemcachedStorageService(@Nonnull final MemcachedClient client, @Positive int timeout) {
        Constraint.isNotNull(client, "Client cannot be null");
        Constraint.isGreaterThan(0, timeout, "Operation timeout must be positive");
        this.client = client;
        this.timeout = timeout;
    }

    @Override
    @Nonnull
    public StorageCapabilities getCapabilities() {
        return capabilities;
    }

    /**
     * Sets the storage capabilities. This method should be used when the default 1M slab size is changed;
     * the {@link edu.vt.middleware.idp.storage.MemcachedStorageCapabilities#valueSize} should be set equal to the
     * chosen slab size.
     *
     * @param capabilities Memcached storage capabilities.
     */
    public void setCapabilities(@Nonnull final MemcachedStorageCapabilities capabilities) {
        Constraint.isNotNull(capabilities, "Storage capabilities cannot be null");
        this.capabilities = capabilities;
    }

    @Override
    public boolean create(@Nonnull @NotEmpty final String context,
                          @Nonnull @NotEmpty final String key,
                          @Nonnull @NotEmpty final String value,
                          @Nullable @Positive final Long expiration) throws IOException {
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(key), "Key cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(value), "Value cannot be null or empty");
        Constraint.isGreaterThan(-1, seconds(expiration), "Expiration must be null or positive");
        String namespace = lookupNamespace(context);
        if (namespace == null) {
            namespace = createNamespace(context);
        }
        final String cacheKey = memcachedKey(namespace, key);
        final MemcachedStorageRecord record = new MemcachedStorageRecord(value, expiration);
        this.logger.debug("Creating new entry at {} for context={}, key={}", cacheKey, context, key);
        return handleAsyncResult(
                this.client.add(cacheKey, record.getMemcachedExpiration(), record, storageRecordTranscoder));
    }

    @Override
    public boolean create(@Nonnull @NotEmpty final String context,
                          @Nonnull @NotEmpty final String key,
                          @Nonnull final Object value,
                          @Nonnull final StorageSerializer serializer,
                          @Nullable @Positive final Long expiration) throws IOException {
        Constraint.isNotNull(serializer, "Serializer cannot be null");
        return create(context, key, serializer.serialize(value), expiration);
    }

    @Override
    public boolean create(@Nonnull Object value) throws IOException {
        Constraint.isNotNull(value, "Value cannot be null");
        return create(
                AnnotationSupport.getContext(value),
                AnnotationSupport.getKey(value),
                AnnotationSupport.getValue(value),
                AnnotationSupport.getExpiration(value));
    }

    @Override
    public StorageRecord read(@Nonnull @NotEmpty final String context,
                              @Nonnull @NotEmpty final String key) throws IOException {
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(key), "Key cannot be null or empty");
        final String namespace = lookupNamespace(context);
        if (namespace == null) {
            this.logger.debug("Namespace for context {} does not exist", context);
            return null;
        }
        final String cacheKey = memcachedKey(namespace, key);
        this.logger.debug("Reading entry at {} for context={}, key={}", cacheKey, context, key);
        final CASValue<MemcachedStorageRecord> record;
        try {
            record = this.client.gets(cacheKey, storageRecordTranscoder);
        } catch (RuntimeException e) {
            throw new IOException("Memcached operation failed", e);
        }
        if (record == null) {
            return null;
        }
        record.getValue().setVersion(record.getCas());
        return record.getValue();
    }

    @Override
    public Object read(@Nonnull final Object value) throws IOException {
        Constraint.isNotNull(value, "Value cannot be null");
        return read(AnnotationSupport.getContext(value), AnnotationSupport.getKey(value));
    }

    @Override
    public Pair<Long , StorageRecord> read(@Nonnull @NotEmpty final String context,
                                           @Nonnull @NotEmpty final String key,
                                           @Positive final long version) throws IOException {
        Constraint.isGreaterThan(0, version, "Version must be positive");
        final StorageRecord record = read(context, key);
        if (record != null) {
            if (version == record.getVersion()) {
                return new Pair<>(version, record);
            } else {
                return new Pair<>(version, null);
            }
        }
        return new Pair<>(null, null);
    }

    @Override
    public boolean update(@Nonnull @NotEmpty final String context,
                          @Nonnull @NotEmpty final String key,
                          @Nonnull @NotEmpty final String value,
                          @Nullable @Positive final Long expiration) throws IOException {
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(key), "Key cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(value), "Value cannot be null or empty");
        Constraint.isGreaterThan(-1, seconds(expiration), "Expiration must be null or positive");
        final String namespace = lookupNamespace(context);
        if (namespace == null) {
            this.logger.debug("Namespace for context {} does not exist", context);
            return false;
        }
        final String cacheKey = memcachedKey(namespace, key);
        final MemcachedStorageRecord record = new MemcachedStorageRecord(value, expiration);
        this.logger.debug("Updating entry at {} for context={}, key={}", cacheKey, context, key);
        return handleAsyncResult(
                this.client.replace(cacheKey, record.getMemcachedExpiration(), record, storageRecordTranscoder));
    }

    @Override
    public boolean update(@Nonnull @NotEmpty final String context,
                          @Nonnull @NotEmpty final String key,
                          @Nonnull final Object value,
                          @Nonnull final StorageSerializer serializer,
                          @Nullable @Positive final Long expiration) throws IOException {
        Constraint.isNotNull(serializer, "Serializer cannot be null");
        return update(context, key, serializer.serialize(value), expiration);
    }

    @Override
    public boolean update(@Nonnull Object value) throws IOException {
        Constraint.isNotNull(value, "Value cannot be null");
        return update(
                AnnotationSupport.getContext(value),
                AnnotationSupport.getKey(value),
                AnnotationSupport.getValue(value),
                AnnotationSupport.getExpiration(value));
    }

    @Override
    public Long updateWithVersion(@Positive final long version,
                                  @Nonnull @NotEmpty final String context,
                                  @Nonnull @NotEmpty final String key,
                                  @Nonnull @NotEmpty final String value,
                                  @Nullable @Positive final Long expiration)
            throws IOException, VersionMismatchException {

        Constraint.isGreaterThan(0, version, "Version must be positive");
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(key), "Key cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(value), "Value cannot be null or empty");
        Constraint.isGreaterThan(-1, seconds(expiration), "Expiration must be null or positive");
        final String namespace = lookupNamespace(context);
        if (namespace == null) {
            this.logger.debug("Namespace for context {} does not exist", context);
            return null;
        }
        final String cacheKey = memcachedKey(namespace, key);
        final MemcachedStorageRecord record = new MemcachedStorageRecord(value, expiration);
        this.logger.debug("Updating entry at {} for context={}, key={}, version={}", cacheKey, context, key, version);
        final CASResponse response = handleAsyncResult(
                this.client.asyncCAS(cacheKey, version, record.getMemcachedExpiration(), record, storageRecordTranscoder));
        Long newVersion = null;
        if (CASResponse.OK == response) {
            final CASValue<MemcachedStorageRecord> newRecord = this.client.gets(cacheKey, storageRecordTranscoder);
            if (newRecord != null) {
                newVersion = newRecord.getCas();
            }
        } else if (CASResponse.EXISTS == response) {
            throw new VersionMismatchException();
        }
        return newVersion;
    }

    @Override
    public Long updateWithVersion(@Positive final long version,
                                  @Nonnull @NotEmpty final String context,
                                  @Nonnull @NotEmpty final String key,
                                  @Nonnull final Object value,
                                  @Nonnull final StorageSerializer serializer,
                                  @Nullable @Positive final Long expiration)
            throws IOException, VersionMismatchException {

        Constraint.isNotNull(serializer, "Serializer cannot be null");
        return updateWithVersion(version, context, key, serializer.serialize(value), expiration);
    }

    @Override
    public Long updateWithVersion(@Positive final long version, @Nonnull final Object value)
            throws IOException, VersionMismatchException {

        Constraint.isNotNull(value, "Value cannot be null");
        return updateWithVersion(
                version,
                AnnotationSupport.getContext(value),
                AnnotationSupport.getKey(value),
                AnnotationSupport.getValue(value),
                AnnotationSupport.getExpiration(value));
    }

    @Override
    public boolean updateExpiration(@Nonnull @NotEmpty final String context,
                                    @Nonnull @NotEmpty final String key,
                                    @Nullable @Positive final Long expiration) throws IOException {
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(key), "Key cannot be null or empty");
        final int seconds = seconds(expiration);
        Constraint.isGreaterThan(-1, seconds, "Expiration must be null or positive");
        final String namespace = lookupNamespace(context);
        if (namespace == null) {
            this.logger.debug("Namespace for context {} does not exist", context);
            return false;
        }
        final String cacheKey = memcachedKey(namespace, key);
        this.logger.debug("Updating expiration for entry at {} for context={}, key={}", cacheKey, context, key);
        return handleAsyncResult(this.client.touch(cacheKey, seconds));
    }

    @Override
    public boolean updateExpiration(@Nonnull final Object value) throws IOException {
        Constraint.isNotNull(value, "Value cannot be null");
        return updateExpiration(
                AnnotationSupport.getContext(value),
                AnnotationSupport.getKey(value),
                AnnotationSupport.getExpiration(value));
    }

    @Override
    public boolean delete(
            @Nonnull @NotEmpty final String context,
            @Nonnull @NotEmpty final String key) throws IOException {
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(key), "Key cannot be null or empty");
        final String namespace = lookupNamespace(context);
        if (namespace == null) {
            this.logger.debug("Namespace for context {} does not exist", context);
            return false;
        }
        final String cacheKey = memcachedKey(namespace, key);
        this.logger.debug("Deleting entry at {} for context={}, key={}", cacheKey, context, key);
        return handleAsyncResult(this.client.delete(cacheKey));
    }

    @Override
    public boolean delete(@Nonnull final Object value) throws IOException {
        Constraint.isNotNull(value, "Value cannot be null");
        return delete(
                AnnotationSupport.getContext(value),
                AnnotationSupport.getKey(value));
    }

    @Override
    public boolean deleteWithVersion(@Positive final long version,
                                     @Nonnull @NotEmpty final String context,
                                     @Nonnull @NotEmpty final String key) throws IOException, VersionMismatchException {
        Constraint.isGreaterThan(0, version, "Version must be positive");
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        Constraint.isNotNull(StringSupport.trimOrNull(key), "Key cannot be null or empty");
        final String namespace = lookupNamespace(context);
        if (namespace == null) {
            this.logger.debug("Namespace for context {} does not exist", context);
            return false;
        }
        final String cacheKey = memcachedKey(namespace, key);
        this.logger.debug("Deleting entry at {} for context={}, key={}, version={}", cacheKey, context, key, version);
        return handleAsyncResult(this.client.delete(cacheKey, version));
    }

    @Override
    public boolean deleteWithVersion(@Positive final long version, @Nonnull final Object value)
            throws IOException, VersionMismatchException {

        Constraint.isNotNull(value, "Value cannot be null");
        return deleteWithVersion(
                version,
                AnnotationSupport.getContext(value),
                AnnotationSupport.getKey(value));
    }

    @Override
    public void reap(@Nonnull @NotEmpty final String context) throws IOException {
        return;
    }

    @Override
    public void updateContextExpiration(@Nonnull @NotEmpty final String context, @Nullable final Long expiration)
            throws IOException {
        throw new UnsupportedOperationException("updateContextExpiration not supported");
    }

    @Override
    public void deleteContext(@Nonnull @NotEmpty final String context) throws IOException {
        Constraint.isNotNull(StringSupport.trimOrNull(context), "Context cannot be null or empty");
        final String namespace = lookupNamespace(context);
        if (namespace == null) {
            this.logger.debug("Namespace for context {} does not exist. Context values effectively deleted.", context);
            return;
        }
        final OperationFuture<Boolean> ctxResult = this.client.delete(context);
        final OperationFuture<Boolean> nsResult = this.client.delete(namespace);
        handleAsyncResult(ctxResult);
        handleAsyncResult(nsResult);
    }

    @Override
    protected void doDestroy() {
        client.shutdown();
    }


    private String lookupNamespace(final String context) throws IOException {
        try {
            return this.client.gets(memcachedKey(context), stringTranscoder).getValue();
        } catch (RuntimeException e) {
            throw new IOException("Memcached operation failed", e);
        }
    }

    private String createNamespace(final String context) throws IOException {
        String namespace =  null;
        boolean success = false;
        // Perform successive add operations until success to ensure unique namespace
        while (!success) {
            namespace = CodecUtil.hex(ByteUtil.toBytes(System.currentTimeMillis()));
            // Namespace values are safe for memcached keys
            success = handleAsyncResult(this.client.add(namespace, 0, context, stringTranscoder));
        }
        // Create the reverse mapping to support looking up namespace by context name
        if (!handleAsyncResult(this.client.add(memcachedKey(context), 0, namespace, stringTranscoder))) {
            throw new IllegalStateException(context + " already exists");
        }
        return namespace;
    }

    /**
     * Creates a memcached key from one or more parts.
     *
     * @param parts Key parts (i.e. namespace, local name)
     *
     * @return Key comprised of 250 characters or less.
     */
    private String memcachedKey(final String ... parts) {
        final String key;
        if (parts.length > 0) {
            final StringBuilder sb = new StringBuilder();
            int i = 0;
            for (String part : parts) {
                if (i++ > 0) {
                    sb.append(':');
                }
                sb.append(part);
            }
            key = sb.toString();
        } else {
            key = parts[0];
        }
        if (key.length() > MAX_KEY_LENGTH) {
            return CodecUtil.hex(HashUtil.sha512(key));
        }
        return key;
    }

    private <T> T handleAsyncResult(final OperationFuture<T> result) throws IOException {
        try {
            return result.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IOException("Memcached operation interrupted");
        } catch (TimeoutException e) {
            throw new IOException("Memcached operation did not complete in time (" + timeout + "s)");
        } catch (ExecutionException e) {
            throw new IOException("Memcached operation error", e);
        }
    }

    private int seconds(final Long millis) {
        return millis == null ? 0 : (int) (millis.longValue() / 1000);
    }
}
