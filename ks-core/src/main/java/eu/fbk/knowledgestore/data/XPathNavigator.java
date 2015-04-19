package eu.fbk.knowledgestore.data;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;

import org.jaxen.DefaultNavigator;
import org.jaxen.UnsupportedAxisException;
import org.jaxen.saxpath.SAXPathException;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

final class XPathNavigator extends DefaultNavigator {

    public static final XPathNavigator INSTANCE = new XPathNavigator();

    private static final long serialVersionUID = -1402394846594186887L;

    private XPathNavigator() {
    }

    public Object wrap(final Object node) {
        return new Element(null, null, node);
    }

    public Object unwrap(final Object object) {
        return object instanceof Element ? ((Element) object).getContent() : object;
    }

    @Override
    public Iterator<?> getChildAxisIterator(final Object contextNode)
            throws UnsupportedAxisException {

        if (contextNode instanceof Element) {
            final Element element = (Element) contextNode;
            if (element.getContent() instanceof Record) {
                return new ChildIterator(element);
            } else {
                final Object value = element.getContent();
                return Iterators.singletonIterator(value instanceof Number || //
                        value instanceof Boolean ? value : value.toString());
            }
        } else {
            return Collections.emptyIterator();
        }
    }

    @Override
    public Iterator<?> getParentAxisIterator(final Object contextNode)
            throws UnsupportedAxisException {
        return contextNode instanceof Element ? new ParentIterator((Element) contextNode)
                : Collections.emptyIterator();
    }

    @Override
    public Object getParentNode(final Object child) throws UnsupportedAxisException {
        return child instanceof Element ? ((Element) child).getParent() : null;
    }

    @Override
    public Object getDocumentNode(final Object contextNode) {
        if (!(contextNode instanceof Element)) {
            return null;
        }
        Element element = (Element) contextNode;
        while (element.getParent() != null) {
            element = element.getParent();
            assert element != null;
        }
        return element;
    }

    @Override
    public String translateNamespacePrefixToUri(final String prefix, final Object element) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public String getElementNamespaceUri(final Object element) {
        if (element instanceof Element) {
            final URI tag = ((Element) element).getTag();
            return tag == null ? "" : tag.getNamespace();
        }
        return null;
    }

    @Override
    public String getElementName(final Object element) {
        if (element instanceof Element) {
            final URI tag = ((Element) element).getTag();
            return tag == null ? "" : tag.getLocalName();
        }
        return null;
    }

    @Override
    public String getElementQName(final Object element) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public String getAttributeNamespaceUri(final Object attr) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public String getAttributeName(final Object attr) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public String getAttributeQName(final Object attr) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public boolean isDocument(final Object object) {
        return object instanceof Element && ((Element) object).getParent() == null;
    }

    @Override
    public boolean isElement(final Object object) {
        return object instanceof Element && ((Element) object).getParent() != null;
    }

    @Override
    public boolean isAttribute(final Object object) {
        return false;
    }

    @Override
    public boolean isNamespace(final Object object) {
        return false;
    }

    @Override
    public boolean isComment(final Object object) {
        return false;
    }

    @Override
    public boolean isText(final Object object) {
        return !(object instanceof Element) && !(object instanceof Collection);
    }

    @Override
    public boolean isProcessingInstruction(final Object object) {
        return false;
    }

    @Override
    public String getCommentStringValue(final Object comment) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public String getElementStringValue(final Object element) {
        if (element instanceof Element) {
            final Object object = ((Element) element).getContent();
            if (object instanceof Record) {
                return "";
            } else if (object instanceof Value) {
                return ((Value) object).stringValue();
            } else if (object instanceof Statement) {
                return object.toString();
            }
        }
        return null;
    }

    @Override
    public String getAttributeStringValue(final Object attr) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public String getNamespaceStringValue(final Object ns) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public String getTextStringValue(final Object text) {
        return text instanceof Element || text instanceof Collection ? null : text.toString();
    }

    @Override
    public String getNamespacePrefix(final Object ns) {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    @Override
    public org.jaxen.XPath parseXPath(final String xpath) throws SAXPathException {
        throw new Error("This method is not expected to be called by Jaxen (!)");
    }

    private static final class ParentIterator extends UnmodifiableIterator<Object> {

        private final Element node;

        ParentIterator(final Element node) {
            assert node != null;
            this.node = node;
        }

        @Override
        public boolean hasNext() {
            return this.node.getParent() != null;
        }

        @Override
        public Element next() {
            final Element parent = this.node.getParent();
            if (parent == null) {
                throw new NoSuchElementException();
            }
            return parent;
        }

    }

    private static final class ChildIterator extends UnmodifiableIterator<Object> {

        private final Element parent;

        private final List<URI> properties;

        private URI propertyURI;

        private int propertyIndex;

        private List<? extends Object> values;

        private int valueIndex;

        public ChildIterator(final Element parent) {

            assert parent != null;
            assert parent.getContent() instanceof Record;

            this.parent = parent;
            this.properties = ((Record) parent.getContent()).getProperties();
            this.propertyURI = null;
            this.propertyIndex = 0;
            this.values = Collections.emptyList();
            this.valueIndex = 0;
        }

        @Override
        public boolean hasNext() {
            return this.valueIndex < this.values.size()
                    || this.propertyIndex < this.properties.size();
        }

        @Override
        public Element next() {
            if (this.valueIndex >= this.values.size()) {
                this.propertyURI = this.properties.get(this.propertyIndex++);
                this.values = ((Record) this.parent.getContent()).get(this.propertyURI);
                this.valueIndex = 0;
            }
            return new Element(this.parent, this.propertyURI, this.values.get(this.valueIndex++));
        }

    }

    private static final class Element {

        @Nullable
        private final Element parent;

        @Nullable
        private final URI tag;

        private final Object content;

        public Element(@Nullable final Element parent, @Nullable final URI tag, //
                final Object content) {
            this.parent = parent;
            this.tag = tag;
            this.content = content;
        }

        @Nullable
        public Element getParent() {
            return this.parent;
        }

        @Nullable
        public URI getTag() {
            return this.tag;
        }

        public Object getContent() {
            return this.content;
        }

    }

}
