package eu.fbk.knowledgestore.populator.rdf;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;

import eu.fbk.knowledgestore.data.Handler;
import eu.fbk.knowledgestore.data.Record;
import eu.fbk.knowledgestore.vocabulary.CKR;

public class Decoder implements Handler<Statement> {

    private final Handler<? super Record> axiomHandler; // axioms to be emitted here

    private final URI globalURI;

    Decoder(final Handler<? super Record> axiomHandler, @Nullable final URI globalURI) {
        this.axiomHandler = Preconditions.checkNotNull(axiomHandler);
        this.globalURI = Objects.firstNonNull(globalURI, CKR.GLOBAL);
    }

    @Override
    public void handle(final Statement element) throws Throwable {
        // TODO
        if (element == null) {
            // end of sequence
            this.axiomHandler.handle(null);
        } else {
            // process it
        }
    }

}
