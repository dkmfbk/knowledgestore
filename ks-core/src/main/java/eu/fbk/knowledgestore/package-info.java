// TODO: revise documentation
/**
 * KnowledgeStore core API ({@code ks-core}).
 * 
 * <h3>Data model</h3>
 * <p>
 * A total ordering relation is defined on nodes, allowing to compare any node against any other
 * node. This relation generalizes the single ordering relation that can be naturally defined for
 * specific types of nodes, and is provided for ease of use (e.g., to provide a canonical way to
 * order nodes) and for simplifying the definition of conditions on nodes. More in details, scalar
 * nodes are different and are sorted before record nodes, while specific rules are applied for
 * ordering among scalars and among records (see, respectively, the documentation for
 * {@link Record} and {@link Scalar}).
 * </p>
 * <p>
 * <b>Resources</b>. A resource is a self-contained piece of unstructured content, such as a news
 * article or a multimedia object, having some descriptive metadata (e.g., from
 * {@link org.openrdf.model.vocabulary.DCTERMS} vocabulary), a binary representation (
 * {@link eu.fbk.knowledgestore.vocabulary.KS#STORED_AS}) and zero or more mentions contained
 * within it ({@link eu.fbk.knowledgestore.vocabulary.KS#HAS_MENTION} ). Note that (1)
 * representation metadata is generated automatically by the system based on the uploaded content,
 * so there is no need to manually set it; and (2) the actual binary data is not available through
 * the resource record object, but must be instead accessed using other API methods of the
 * KnowledgeStore.
 * </p>
 * <p>
 * <b>Representations</b>. A digital representation is possibly stored for each resource in the
 * KnowledgeStore. A {@code Representation} record contains the metadata of such a representation,
 * while its actual binary content can be accessed via the {@code Representation} class using
 * specific methods of the KnowledgeStore API. The representation metadata stored by this kind of
 * record comprise the filename ({@link eu.fbk.knowledgestore.vocabulary.NFO#FILE_NAME}), the size
 * in bytes ( {@link eu.fbk.knowledgestore.vocabulary.NFO#FILE_SIZE} ), the date and time the
 * representation was created ({@link eu.fbk.knowledgestore.vocabulary.NFO#FILE_CREATED}) and the
 * MIME type (see {@link eu.fbk.knowledgestore.vocabulary.NIE#MIME_TYPE}). Representation records
 * are automatically created by the system for stored resources.
 * </p>
 * <p>
 * <b>Mentions</b>. A mention is a snippet of a resource (
 * {@link eu.fbk.knowledgestore.vocabulary.KS#MENTION_OF}), such as some characters in a text
 * document or some pixels in an image, that may refer to some entity of interest (
 * {@link eu.fbk.knowledgestore.vocabulary.KS#REFERS_TO}). Mentions can be automatically extracted
 * by natural language and multimedia processing tools, that can enrich them with additional
 * attributes about how they denote their referent (e.g., name, qualifiers, 'sentiment').
 * Therefore, mentions present both unstructured and structured facets not available in resources
 * and entities layers alone, and are thus a valuable source of information on their own. A number
 * of axioms ( {@link eu.fbk.knowledgestore.vocabulary.KS#EXPRESSES}) may be expressed by the
 * mention, i.e., can be extracted from its snippet and attributes.
 * </p>
 * <p>
 * <b>Entities</b>. An entity is described by a (possibly empty) set of <i>axioms</i> (
 * {@link eu.fbk.knowledgestore.vocabulary.KS#DESCRIBED_BY}) and is referred by a (possibly empty)
 * set of <i>mentions</i> ( {@link eu.fbk.knowledgestore.vocabulary.KS#REFERRED_BY}).
 * </p>
 * <p>
 * <b>Axioms</b>. Axioms describe entities of the KnowledgeStore. An axiom is encoded by a set of
 * RDF statements ({@link eu.fbk.knowledgestore.vocabulary.KS#ENCODED_BY}) that relate one or more
 * entities and its validity may be restricted to a specific <it>context</it> (
 * {@link eu.fbk.knowledgestore.vocabulary.KS#HOLDS_IN}); an axiom may be annotated with
 * additional metadata and can be expressed by a number of <i>mentions</i> (
 * {@link eu.fbk.knowledgestore.vocabulary.KS#EXPRESSED_BY}). Note that a null context is used in
 * case of axioms holding universally and not in a specific context. An axiom may be <i>simple</i>
 * or <i>complex</i>, with the first consisting of exactly one RDF statement (possibly
 * contextualized and associated to additional metadata); simple axioms are generally used for
 * ABox assertions, while complex axioms are associated mainly to TBox declarations. Axioms IDs
 * should be assigned based on RDF statements and context.
 * </p>
 * <p>
 * <b>Contexts</b> A context defines the circumstances under which a certain axiom holds; each
 * axiom is associated exactly to a context in the KnowledgeStore (which may be shared by multiple
 * axioms) via the {@link eu.fbk.knowledgestore.vocabulary.KS#HOLDS_IN} property (if null, the
 * axiom holds universally). A context is identified by a set of attributes or <i>contextual
 * dimensions</i>, which are the properties of the context record. IDs of context records should
 * be assigned based on all the values of all the contextual dimensions.
 * </p>
 */
@javax.annotation.ParametersAreNonnullByDefault
package eu.fbk.knowledgestore;

import eu.fbk.knowledgestore.data.Record;

// NOTES ON AXIOMS
// - may add enum Axiom.Kind and method Kind getKind() for retrieving the type of ABox/TBox
// axiom.
// Types would be
// SUB_CLASS rdfs:subClassOf
// EQUIVALENT_CLASSES owl:equivalentClass // also for DatatypeDefinition
// DISJOINT_CLASSES owl:disjointWith, owl:AllDisjointClasses**** (type)
// DISJOINT_UNION owl:disjointUnionOf***
// SUB_PROPERTY rdfs:subPropertyOf
// PROPERTY_CHAIN owl:propertyChainAxiom***
// EQUIVALENT_PROPERTIES owl:equivalentProperty
// DISJOINT_PROPERTIES owl:propertyDisjointWith, owl:AllDisjointProperties*** (type)
// PROPERTY_DOMAIN rdfs:domain
// PROPERTY_RANGE rdfs:range
// INVERSE_PROPERTIES owl:inverseOf
// FUNCTIONAL_PROPERTY owl:FunctionalProperty
// INVERSE_FUNCTIONAL_PROPERTY owl:InverseFunctionalProperty
// REFLEXIVE_PROPERTY owl:ReflexiveProperty
// IRREFLEXIVE_PROPERTY owl:IrreflexiveProperty
// SYMMETRIC_PROPERTY owl:SymmetricProperty
// ASYMMETRIC_PROPERTY owl:AsymmetricProperty
// TRANSITIVE_PROPERTY owl:TransitiveProperty
// KEY owl:hasKey***
// SAME_AS owl:sameAs
// DIFFERENT_FROM owl:differentFrom, owl:AllDifferent***
// CLASS_ASSERTION rdf:type
// PROPERTY_ASSERTION ???
// NEGATIVE_PROPERTY_ASSERTION owl:NegativePropertyAssertion
