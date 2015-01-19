package eu.fbk.knowledgestore.datastore.hbase.exception;

import java.io.IOException;

import javax.annotation.Nullable;

public class DataTransactionBlockingException extends IOException {

    /**
     * Serialization number
     */
    private static final long serialVersionUID = 7782565429476207178L;

    /**
     * Creates a new instance with the filename and additional error message specified.
     * 
     * @param classname the class name where the exception was raised.
     * @param message provided additional information, which is concatenated to
     */
    public DataTransactionBlockingException(final String classname, @Nullable final String message) {
        super("Failed while processing a data transaction inside " + classname + "." + (message == null ? "" : " " + message));
    }
}
