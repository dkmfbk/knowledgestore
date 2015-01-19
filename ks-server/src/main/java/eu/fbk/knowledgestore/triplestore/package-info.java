/**
 * {@code TripleStore} server-side component API ({@code ks-server}).
 * <p>
 * This package defines the abstract API of the {@code TripleStore} internal server-side
 * component, whose task is to support the scalable storage of RDF triples within named graphs
 * (which can be also seen as quadruples), providing reasoning and SPARQL querying facilities on
 * top of it. More in details, the package provides:
 * </p>
 * <ul>
 * <li>the {@code TripleStore} API ({@link eu.fbk.knowledgestore.triplestore.TripleStore},
 * {@link eu.fbk.knowledgestore.triplestore.TripleTransaction},
 * {@link eu.fbk.knowledgestore.triplestore.SelectQuery});</li>
 * <li>abstract classes ({@link eu.fbk.knowledgestore.triplestore.ForwardingTripleStore} and
 * {@link eu.fbk.knowledgestore.triplestore.ForwardingTripleTransaction}) for implementing the
 * decorator pattern;</li>
 * <li>two concrete decorator classes providing, respectively, logging support (
 * {@link eu.fbk.knowledgestore.triplestore.LoggingTripleStore}) and synchronization support (
 * {@link eu.fbk.knowledgestore.triplestore.SynchronizedTripleStore}).</li>
 * </ul>
 * </p>
 * <p>
 * Note that component API builds on Sesame; however, it does not reuse the Repository or Sail
 * concepts of Sesame, but defines a more lightweight {@code TripleStore} interface that allows
 * the implementation of the component on top of a wider range of existing triple store systems.
 * Component implementations for specific triple stores are provided by dedicated modules.
 * </p>
 */
@javax.annotation.ParametersAreNonnullByDefault
package eu.fbk.knowledgestore.triplestore;

