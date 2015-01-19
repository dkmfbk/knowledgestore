package eu.fbk.knowledgestore.runtime;

import java.io.IOException;

/**
 * Signals a non-recoverable data corruption situation.
 * <p>
 * This exception is thrown by operation operating on persistent data structures that detect that
 * those structures are missing or corrupted. The expected recovery procedure consists in
 * reinitializing those structures and re-populating them: this can be done by the KnowledgeStore
 * only for some auxiliary structures, while in the general case manual intervention from an
 * administrator is required.
 * </p>
 * <p>
 * This exception is introduced to differentiate from other <tt>IOException</tt> that do not imply
 * a corruption of stored data, and as such may be addressed by fixing their cause and re-attempt
 * the operation. A <tt>DataCorruptedException</tt> provides a trigger for recovery procedures
 * possibly performed automatically by the system or some external code accessing it.
 * </p>
 */
public class DataCorruptedException extends IOException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance with the optional error message specified.
     * 
     * @param message
     *            an optional error message
     */
    public DataCorruptedException(final String message) {
        this(message, null);
    }

    /**
     * Creates a new instance with the optional error message and cause specified.
     * 
     * @param message
     *            an optional message providing additional information
     * @param cause
     *            the optional cause of this exception
     */
    public DataCorruptedException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
