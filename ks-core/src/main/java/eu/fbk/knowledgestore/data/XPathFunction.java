package eu.fbk.knowledgestore.data;

import java.util.List;

import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.jaxen.Context;
import org.jaxen.Function;
import org.jaxen.FunctionCallException;
import org.jaxen.XPathFunctionContext;
import org.jaxen.function.BooleanFunction;
import org.jaxen.function.CeilingFunction;
import org.jaxen.function.ConcatFunction;
import org.jaxen.function.ContainsFunction;
import org.jaxen.function.CountFunction;
import org.jaxen.function.FalseFunction;
import org.jaxen.function.FloorFunction;
import org.jaxen.function.IdFunction;
import org.jaxen.function.LangFunction;
import org.jaxen.function.LastFunction;
import org.jaxen.function.LocalNameFunction;
import org.jaxen.function.NameFunction;
import org.jaxen.function.NamespaceUriFunction;
import org.jaxen.function.NormalizeSpaceFunction;
import org.jaxen.function.NotFunction;
import org.jaxen.function.NumberFunction;
import org.jaxen.function.PositionFunction;
import org.jaxen.function.RoundFunction;
import org.jaxen.function.StartsWithFunction;
import org.jaxen.function.StringFunction;
import org.jaxen.function.StringLengthFunction;
import org.jaxen.function.SubstringAfterFunction;
import org.jaxen.function.SubstringBeforeFunction;
import org.jaxen.function.SubstringFunction;
import org.jaxen.function.SumFunction;
import org.jaxen.function.TranslateFunction;
import org.jaxen.function.TrueFunction;
import org.jaxen.function.ext.EndsWithFunction;
import org.jaxen.function.ext.EvaluateFunction;
import org.jaxen.function.ext.LowerFunction;
import org.jaxen.function.ext.UpperFunction;
import org.openrdf.model.URI;

// TODO add subject(), predicate(), object(), context() functions to extract statement components

abstract class XPathFunction implements Function {

    static final XPathFunctionContext CONTEXT;

    static {
        CONTEXT = new XPathFunctionContext();

        // Functions on nodes and context
        CONTEXT.registerFunction(null, "position", new PositionFunction()); // xpath1
        CONTEXT.registerFunction(null, "last", new LastFunction()); // xpath1
        CONTEXT.registerFunction(null, "id", new IdFunction()); // xpath1
        CONTEXT.registerFunction(null, "name", new NameFunction()); // xpath1
        CONTEXT.registerFunction(null, "local-name", new LocalNameFunction()); // xpath1
        CONTEXT.registerFunction(null, "namespace-uri", new NamespaceUriFunction()); // xpath1
        CONTEXT.registerFunction(null, "lang", new LangFunction()); // xpath1
        CONTEXT.registerFunction(null, "evaluate", new EvaluateFunction()); // xpath2 jaxen

        // Function on RDF Values
        CONTEXT.registerFunction(null, "uri", new URIFunction());
        CONTEXT.registerFunction(null, "escape-uri", new EscapeURIFunction());
        CONTEXT.registerFunction(null, "str", new StrFunction());
        CONTEXT.registerFunction(null, "strdt", new StrdtFunction());
        CONTEXT.registerFunction(null, "strlang", new StrlangFunction());

        // Functions on booleans
        CONTEXT.registerFunction(null, "boolean", new BooleanFunction()); // xpath1
        CONTEXT.registerFunction(null, "true", new TrueFunction()); // xpath1
        CONTEXT.registerFunction(null, "false", new FalseFunction()); // xpath1
        CONTEXT.registerFunction(null, "not", new NotFunction()); // xpath1
        CONTEXT.registerFunction(null, "abs", new AbsFunction());
        CONTEXT.registerFunction(null, "power", new PowerFunction());

        // Functions on numbers
        CONTEXT.registerFunction(null, "number", new NumberFunction()); // xpath1
        CONTEXT.registerFunction(null, "ceiling", new CeilingFunction()); // xpath1
        CONTEXT.registerFunction(null, "floor", new FloorFunction()); // xpath1
        CONTEXT.registerFunction(null, "round", new RoundFunction()); // xpath1

        // Functions on strings
        CONTEXT.registerFunction(null, "concat", new ConcatFunction()); // xpath1
        CONTEXT.registerFunction(null, "contains", new ContainsFunction()); // xpath1
        CONTEXT.registerFunction(null, "normalize-space", new NormalizeSpaceFunction()); // xpath1
        CONTEXT.registerFunction(null, "starts-with", new StartsWithFunction()); // xpath1
        CONTEXT.registerFunction(null, "string", new StringFunction()); // xpath1
        CONTEXT.registerFunction(null, "string-length", new StringLengthFunction()); // xpath1
        CONTEXT.registerFunction(null, "substring-after", new SubstringAfterFunction()); // xpath1
        CONTEXT.registerFunction(null, "substring-before", new SubstringBeforeFunction()); // xpath1
        CONTEXT.registerFunction(null, "substring", new SubstringFunction()); // xpath1
        CONTEXT.registerFunction(null, "translate", new TranslateFunction()); // xpath1
        CONTEXT.registerFunction(null, "ends-with", new EndsWithFunction()); // xpath2 jaxen
        CONTEXT.registerFunction(null, "lower-case", new LowerFunction()); // xpath2 jaxen
        CONTEXT.registerFunction(null, "upper-case", new UpperFunction()); // xpath2 jaxen
        CONTEXT.registerFunction(null, "compare", new CompareFunction());
        CONTEXT.registerFunction(null, "string-join", new StringJoinFunction());
        CONTEXT.registerFunction(null, "matches", new MatchesFunction());
        CONTEXT.registerFunction(null, "replace", new ReplaceFunction());
        CONTEXT.registerFunction(null, "tokenize", new TokenizeFunction());

        // Functions on dates
        CONTEXT.registerFunction(null, "dateTime", new DateTimeFunction());
        CONTEXT.registerFunction(null, "current-dateTime", new CurrentDateTimeFunction());
        CONTEXT.registerFunction(null, "year-from-dateTime", new YearFromDateTimeFunction());
        CONTEXT.registerFunction(null, "month-from-dateTime", new MonthFromDateTimeFunction());
        CONTEXT.registerFunction(null, "day-from-dateTime", new DayFromDateTimeFunction());
        CONTEXT.registerFunction(null, "hours-from-dateTime", new HoursFromDateTimeFunction());
        CONTEXT.registerFunction(null, "minutes-from-dateTime", new MinutesFromDateTimeFunction());
        CONTEXT.registerFunction(null, "seconds-from-dateTime", new SecondsFromDateTimeFunction());
        CONTEXT.registerFunction(null, "timezone-from-dateTime",
                new TimeZoneFromDateTimeFunction());

        // Functions on sequences
        CONTEXT.registerFunction(null, "count", new CountFunction()); // xpath1
        CONTEXT.registerFunction(null, "sum", new SumFunction()); // xpath1
        CONTEXT.registerFunction(null, "sequence", new SequenceFunction());
        CONTEXT.registerFunction(null, "subsequence", new SubsequenceFunction());
        CONTEXT.registerFunction(null, "index-of", new IndexOfFunction());
        CONTEXT.registerFunction(null, "insert-before", new InsertBeforeFunction());
        CONTEXT.registerFunction(null, "remove", new RemoveFunction());
        CONTEXT.registerFunction(null, "reverse", new ReverseFunction());
        CONTEXT.registerFunction(null, "distinct-values", new DistinctValuesFunction());
        CONTEXT.registerFunction(null, "exists", new ExistsFunction());
        CONTEXT.registerFunction(null, "empty", new EmptyFunction());
        CONTEXT.registerFunction(null, "avg", new AvgFunction());
        CONTEXT.registerFunction(null, "max", new MaxFunction());
        CONTEXT.registerFunction(null, "min", new MinFunction());
    }

    // FUNCTIONS ON URIS

    private static final class URIFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {

            if (args.size() != 1) {
                throw new FunctionCallException("uri() requires one argument");
            }

            final String string = StringFunction.evaluate(args.get(0), context.getNavigator());
            return Data.getValueFactory().createURI(string);
        }

    }

    private static final class StrFunction extends XPathFunction {

        @SuppressWarnings("rawtypes")
        @Override
        public Object call(final Context context, final List args) throws FunctionCallException {

            if (args.size() != 1) {
                throw new FunctionCallException("str() requires one argument");
            }

            final String label = StringFunction.evaluate(args.get(0), context.getNavigator());
            return Data.getValueFactory().createLiteral(label);
        }

    }

    private static final class StrdtFunction extends XPathFunction {

        @SuppressWarnings("rawtypes")
        @Override
        public Object call(final Context context, final List args) throws FunctionCallException {

            if (args.size() != 2) {
                throw new FunctionCallException("strdt() requires two arguments");
            }

            final String label = StringFunction.evaluate(args.get(0), context.getNavigator());
            final URI dt = args.get(1) instanceof URI ? (URI) args.get(1) : Data.getValueFactory()
                    .createURI(StringFunction.evaluate(args.get(1), context.getNavigator()));
            return Data.getValueFactory().createLiteral(label, dt);
        }

    }

    private static final class StrlangFunction extends XPathFunction {

        @SuppressWarnings("rawtypes")
        @Override
        public Object call(final Context context, final List args) throws FunctionCallException {

            if (args.size() != 2) {
                throw new FunctionCallException("strlang() requires two arguments");
            }

            final String label = StringFunction.evaluate(args.get(0), context.getNavigator());
            final String lang = StringFunction.evaluate(args.get(1), context.getNavigator());
            return Data.getValueFactory().createLiteral(label, lang);
        }

    }

    private static final class EscapeURIFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    // FUNCTIONS ON NUMBERS

    private static final class AbsFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class PowerFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    // FUNCTIONS ON STRINGS

    private static final class CompareFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class StringJoinFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class MatchesFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class ReplaceFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class TokenizeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    // FUNCTIONS ON DATES

    private static final class DateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            return Data.convert(args.get(0), XMLGregorianCalendar.class);
        }

    }

    private static final class CurrentDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class YearFromDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class MonthFromDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class DayFromDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class HoursFromDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class MinutesFromDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class SecondsFromDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class TimeZoneFromDateTimeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    // FUNCTIONS ON SEQUENCES

    private static final class SequenceFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {

            final List<Object> result = Lists.newArrayList();
            for (final Object arg : args) {
                if (arg instanceof Iterable<?>) {
                    Iterables.addAll(result, (Iterable<?>) arg);
                } else {
                    result.add(arg);
                }
            }
            return result;
        }

    }

    private static final class SubsequenceFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class IndexOfFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class InsertBeforeFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class RemoveFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class ReverseFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class DistinctValuesFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class ExistsFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class EmptyFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class AvgFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class MaxFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

    private static final class MinFunction extends XPathFunction {

        @Override
        @SuppressWarnings("rawtypes")
        public Object call(final Context context, final List args) throws FunctionCallException {
            throw new UnsupportedOperationException(); // TODO
        }

    }

}
