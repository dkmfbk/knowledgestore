package eu.fbk.knowledgestore.filestore;

import java.io.IOException;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * Signals an attempt at creating a file that already exists.
 * <p>
 * This exception may denote either an error of the caller (in case it previously created the
 * specified file) or an external modification to the {@link FileStore} resulting in the external
 * creation of a file with the same filename. Mandatory property {@link #getFilename()} provides
 * the name of the existing file.
 * </p>
 * <p>
 * Note: a specific exception has been introduced as the existing
 * {@code java.nio.file.FileAlreadyExistsException} is strictly related to the JDK FileSystem
 * class and to the access to files on OS-managed filesystems.
 * </p>
 */
public class FileExistsException extends IOException {

    private static final long serialVersionUID = 4370301343958349711L;

    private final String filename;

    /**
     * Creates a new instance with the filename and additional error message specified.
     * 
     * @param filename
     *            the filename identifying the existing file
     * @param message
     *            an optional message providing additional information, which is concatenated to
     *            an auto-generated message reporting the filename of the existing file
     * 
     * @see #FileExistsException(String, String, Throwable)
     */
    public FileExistsException(final String filename, @Nullable final String message) {
        this(filename, message, null);
    }

    /**
     * Creates a new instance with the filename, additional error message and cuase specified.
     * 
     * @param filename
     *            the filename identifying the existing file
     * @param message
     *            an optional message providing additional information, which is concatenated to
     *            an auto-generated message reporting the filename of the existing file
     * @param cause
     *            an optional cause of this exception
     */
    public FileExistsException(final String filename, @Nullable final String message,
            @Nullable final Throwable cause) {

        super("File " + filename + " already exists." + (message == null ? "" : " " + message),
                cause);

        Preconditions.checkNotNull(filename);
        this.filename = filename;
    }

    /**
     * Returns the filename identifying the existing file.
     * 
     * @return the filename
     */
    public final String getFilename() {
        return this.filename;
    }

}
