package org.jaxen.expr;

import java.util.Iterator;
import java.util.List;

import org.jaxen.Context;
import org.jaxen.JaxenException;
import org.jaxen.Navigator;
import org.openrdf.model.Value;

import eu.fbk.knowledgestore.data.Data;
import eu.fbk.knowledgestore.data.XPath;

abstract class DefaultRelationalExpr extends DefaultTruthExpr implements RelationalExpr {

    /**
     *
     */
    private static final long serialVersionUID = -3565329669240565813L;

    DefaultRelationalExpr(final Expr lhs, final Expr rhs) {
        super(lhs, rhs);
    }

    @Override
    public String toString() {
        return "[(DefaultRelationalExpr): " + getLHS() + ", " + getRHS() + "]";
    }

    @Override
    public Object evaluate(final Context context) throws JaxenException {
        final Object lhsValue = getLHS().evaluate(context);
        final Object rhsValue = getRHS().evaluate(context);
        final Navigator nav = context.getNavigator();

        if (bothAreSets(lhsValue, rhsValue)) {
            return evaluateSetSet((List) lhsValue, (List) rhsValue, nav);
        }

        if (eitherIsSet(lhsValue, rhsValue)) {
            if (isSet(lhsValue)) {
                return evaluateSetSet((List) lhsValue, convertToList(rhsValue), nav);
            } else {
                return evaluateSetSet(convertToList(lhsValue), (List) rhsValue, nav);
            }
        }

        return evaluateObjectObject(lhsValue, rhsValue, nav) ? Boolean.TRUE : Boolean.FALSE;
    }

    private Object evaluateSetSet(final List lhsSet, final List rhsSet, final Navigator nav) {
        if (setIsEmpty(lhsSet) || setIsEmpty(rhsSet)) // return false if either is null or empty
        {
            return Boolean.FALSE;
        }

        for (final Iterator lhsIterator = lhsSet.iterator(); lhsIterator.hasNext();) {
            final Object lhs = lhsIterator.next();

            for (final Iterator rhsIterator = rhsSet.iterator(); rhsIterator.hasNext();) {
                final Object rhs = rhsIterator.next();

                if (evaluateObjectObject(lhs, rhs, nav)) {
                    return Boolean.TRUE;
                }
            }
        }

        return Boolean.FALSE;
    }

    private boolean evaluateObjectObject(final Object lhs, final Object rhs, final Navigator nav) {

        if (lhs == null || rhs == null) {
            return false;
        }

        final Value lhsValue = Data.convert(XPath.unwrap(lhs), Value.class);
        final Value rhsValue = Data.convert(XPath.unwrap(rhs), Value.class);

        final int result = Data.getTotalComparator().compare(lhsValue, rhsValue);

        switch (getOperator()) {
        case "<":
            return result < 0;
        case ">":
            return result > 0;
        case "=":
            return result == 0;
        case "<=":
            return result <= 0;
        case ">=":
            return result >= 0;
        case "!=":
            return result != 0;
        default:
            return false;
        }
        //
        // final Double lhsNum = NumberFunction.evaluate(lhs, nav);
        // final Double rhsNum = NumberFunction.evaluate(rhs, nav);
        //
        // if (NumberFunction.isNaN(lhsNum) || NumberFunction.isNaN(rhsNum)) {
        // return false;
        // }
        //
        // return evaluateDoubleDouble(lhsNum, rhsNum);
    }

    protected abstract boolean evaluateDoubleDouble(Double lhs, Double rhs);

}
