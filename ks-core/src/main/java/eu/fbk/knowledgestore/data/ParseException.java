package eu.fbk.knowledgestore.data;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * Signals a failure in parsing a string according to some formal grammar.
 * <p>
 * This exception is thrown when a condition, expression or query string, or any other string
 * obeying some formal grammar, cannot be parsed for any reason (e.g., syntax error).
 * </p>
 */
public class ParseException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final String parsedString;

    /**
     * Creates a new instance with the parsed string and the optional error message specified.
     * 
     * @param parsedString
     *            a parsed string, for debugging purposes
     * @param message
     *            an optional error message, to which the supplied query is concatenated
     */
    public ParseException(final String parsedString, @Nullable final String message) {
        this(parsedString, message, null);
    }

    /**
     * Creates a new instance with the query, optional error message and cause specified.
     * 
     * @param parsedString
     *            a parsed string, for debugging purposes
     * @param message
     *            an optional error message, to which the supplied parsed string is concatenated
     * @param cause
     *            the optional cause of this exception
     */
    public ParseException(final String parsedString, @Nullable final String message,
            @Nullable final Throwable cause) {

        super(message + (parsedString == null ? "" : "\nParsed string:\n\n" + parsedString), cause);

        Preconditions.checkNotNull(parsedString);
        this.parsedString = parsedString;
    }

    /**
     * Returns the parsed string. This property is intended for debugging purposes.
     * 
     * @return the parsed string
     */
    public final String getParsedString() {
        return this.parsedString;
    }

}
