//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.4 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2013.12.10 at 04:45:50 PM CET 
//


package eu.fbk.knowledgestore.populator.naf.model;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "factvalue"
})
@XmlRootElement(name = "factualitylayer")
public class Factualitylayer {

    @XmlElement(required = true)
    protected List<Factvalue> factvalue;

    /**
     * Gets the value of the factvalue property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the factvalue property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getFactvalue().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Factvalue }
     * 
     * 
     */
    public List<Factvalue> getFactvalue() {
        if (factvalue == null) {
            factvalue = new ArrayList<Factvalue>();
        }
        return this.factvalue;
    }

}