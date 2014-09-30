package edu.vt.middleware.idp.storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import net.shibboleth.utilities.java.support.annotation.constraint.NotEmpty;
import net.shibboleth.utilities.java.support.annotation.constraint.Positive;
import org.opensaml.storage.StorageRecord;

/**
 * Storage record implementation for use with {@link MemcachedStorageService}.
 *
 * @author Marvin S. Addison
 */
public class MemcachedStorageRecord extends StorageRecord {

    /**
     * Creates a new instance with specific record version.
     *
     * @param val Stored value.
     * @param exp Expiration instant in milliseconds, null for infinite expiration.
     */
    public MemcachedStorageRecord(@Nonnull @NotEmpty String val, @Nullable Long exp) {
        super(val, exp);
    }

    /**
     * Gets the expiration date as an integer representing seconds since the Unix epoch, 1970-01-01T00:00:00.
     *
     * @return 0 if expiration is null, otherwise <code>getExpiration()/1000</code>.
     */
    public int getMemcachedExpiration() {
        return getExpiration() == null ? 0 : (int) (getExpiration() / 1000);
    }

    /**
     * Sets the record version.
     * @param version Record version; must be positive.
     */
    @Override
    protected void setVersion(@Positive final long version) {
        super.setVersion(version);
    }
}
