package eu.fbk.knowledgestore.server.http;

import java.net.URL;
import java.util.Arrays;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import eu.fbk.knowledgestore.internal.Util;

public final class SecurityConfig {

    public static final String ROLE_DOWNLOADER = "downloader";

    public static final String ROLE_CRUD_READER = "crud_reader";

    public static final String ROLE_SPARQL_READER = "sparql_reader";

    public static final String ROLE_WRITER = "writer";

    public static final String ROLE_UI_USER = "ui_user";

    static final Set<String> ALL_ROLES = ImmutableSet.of(ROLE_DOWNLOADER, ROLE_CRUD_READER,
            ROLE_SPARQL_READER, ROLE_WRITER, ROLE_UI_USER);

    @Nullable
    private final String realm;

    private final String userdbLocation;

    private final URL userdbURL;

    private final Set<String> anonymousRoles;

    public SecurityConfig(@Nullable final String realm, final String userdbLocation,
            final String... anonymousRoles) {
        this(realm, userdbLocation, Arrays.asList(anonymousRoles));
    }

    public SecurityConfig(@Nullable final String realm, final String userdbLocation,
            final Iterable<? extends String> anonymousRoles) {
        this.realm = realm;
        this.userdbLocation = Preconditions.checkNotNull(userdbLocation);
        this.userdbURL = Util.getURL(userdbLocation);
        this.anonymousRoles = ImmutableSet.copyOf(anonymousRoles);
        for (final String role : anonymousRoles) {
            Preconditions.checkArgument(ALL_ROLES.contains(role), "Invalid role %s", role);
        }
    }

    @Nullable
    public String getRealm() {
        return this.realm;
    }

    public String getUserdbLocation() {
        return this.userdbLocation;
    }

    public URL getUserdbURL() {
        return this.userdbURL;
    }

    public Set<String> getAnonymousRoles() {
        return this.anonymousRoles;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof SecurityConfig)) {
            return false;
        }
        final SecurityConfig o = (SecurityConfig) object;
        return Objects.equal(this.realm, o.realm) && this.userdbLocation.equals(o.userdbLocation)
                && this.anonymousRoles.equals(o.anonymousRoles);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.realm, this.userdbLocation, this.anonymousRoles);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        if (this.realm != null) {
            builder.append("realm=").append(this.realm).append(", ");
        }
        builder.append("userdb=").append(this.userdbLocation);
        builder.append(", anonymousRoles=").append(Joiner.on(";").join(this.anonymousRoles));
        return super.toString();
    }

}
