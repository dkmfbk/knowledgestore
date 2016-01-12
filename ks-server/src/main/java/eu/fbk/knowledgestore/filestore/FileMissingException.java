package eu.fbk.knowledgestore.filestore;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * Signals an attempt at accessing a non existing file.
 * <p>
 * This exception may denote either an error of the caller (in case it previously created the
 * specified file) or an external modification to the {@code FileStore} resulting in the removal
 * of a file previously created, which thus becomes missing. Mandatory property
 * {@link #getFilename()} provides the name of the missing file.
 * </p>
 * <p>
 * Note: a specific exception has been introduced as the existing
 * {@code java.io.FileNotFoundException} and {@code java.nio.file.NoSuchFileException} are
 * strictly related to JDK classes and the access to files on an OS-managed filesystem.
 * </p>
 */
public class FileMissingException extends IOException {

    private static final long serialVersionUID = -6196913035982664339L;

    private final String filename;

    /**
     * Creates a new instance with the filename and additional error message specified.
     *
     * @param filename
     *            the filename identifying the missing file
     * @param message
     *            an optional message providing additional information, which is concatenated to
     *            an auto-generated message reporting the filename of the missing file
     *
     * @see #FileMissingException(String, String, Throwable)
     */
    public FileMissingException(final String filename, @Nullable final String message) {
        this(filename, message, null);
    }

    /**
     * Creates a new instance with the filename, additional error message and cause specified.
     *
     * @param filename
     *            the filename identifying the missing file
     * @param message
     *            an optional message providing additional information, which is concatenated to
     *            an auto-generated message reporting the filename of the missing file
     * @param cause
     *            an optional cause of this exception
     */
    public FileMissingException(final String filename, @Nullable final String message,
            @Nullable final Throwable cause) {
        super("File " + filename + " does not exist." + (message == null ? "" : " " + message),
                cause);

        Preconditions.checkNotNull(filename);
        this.filename = filename;
    }

    /**
     * Returns the filename identifying the missing file.
     *
     * @return the filename
     */
    public final String getFilename() {
        return this.filename;
    }

}
