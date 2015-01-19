package eu.fbk.knowledgestore.server.http;

import java.net.URL;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import eu.fbk.knowledgestore.internal.Util;

public final class KeystoreConfig {

    private final String location;

    private final URL url;

    private final String password;

    @Nullable
    private final String alias;

    @Nullable
    private final String type;

    public KeystoreConfig(final String location, final String password) {
        this(location, password, null, null);
    }

    public KeystoreConfig(final String location, final String password,
            @Nullable final String alias, @Nullable final String type) {
        this.location = Preconditions.checkNotNull(location);
        this.password = Preconditions.checkNotNull(password);
        this.alias = alias;
        this.type = type;
        this.url = Util.getURL(location);
    }

    public String getLocation() {
        return this.location;
    }

    public URL getURL() {
        return this.url;
    }

    public String getPassword() {
        return this.password;
    }

    @Nullable
    public String getAlias() {
        return this.alias;
    }

    @Nullable
    public String getType() {
        return this.type;
    }

    @Override
    public boolean equals(@Nullable final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof KeystoreConfig)) {
            return false;
        }
        final KeystoreConfig o = (KeystoreConfig) object;
        return this.location.equals(o.location) //
                && this.password.equals(o.password) //
                && Objects.equal(this.alias, o.alias) //
                && Objects.equal(this.type, o.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.location, this.password, this.alias, this.type);
    }

    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder();
        b.append("location=").append(this.location);
        b.append(", password=").append(this.password);
        if (this.alias != null) {
            b.append(", alias=").append(this.alias);
        }
        if (this.type != null) {
            b.append(", type=").append(this.type);
        }
        return b.toString();
    }

}
