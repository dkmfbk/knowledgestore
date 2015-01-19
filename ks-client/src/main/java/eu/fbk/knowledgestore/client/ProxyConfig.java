package eu.fbk.knowledgestore.client;

import java.net.MalformedURLException;
import java.net.URL;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public final class ProxyConfig {

    private final String url;

    @Nullable
    private final String username;

    @Nullable
    private final String password;

    public ProxyConfig(final String url) {
        this(url, null, null);
    }

    public ProxyConfig(final String url, @Nullable final String username,
            @Nullable final String password) {
        this.url = url.trim();
        this.username = username;
        this.password = password;
        try {
            final URL u = new URL(url);
            final String p = u.getProtocol().toLowerCase();
            Preconditions.checkArgument(p.equals("http") || p.equals("https"),
                    "Not an HTTP(S) URL: " + url);
        } catch (final MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid URL: " + url);
        }
    }

    public String getURL() {
        return this.url;
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof ProxyConfig)) {
            return false;
        }
        final ProxyConfig o = (ProxyConfig) object;
        return this.url.equals(o.url) && Objects.equal(this.username, o.username)
                && Objects.equal(this.password, o.password);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.url, this.username, this.password);
    }

    @Override
    public String toString() {
        if (this.username == null && this.password == null) {
            return this.url;
        }
        final String info = Strings.nullToEmpty(this.username) + ":"
                + Strings.nullToEmpty(this.password);
        return this.url.replaceFirst("://", info);
    }

}
