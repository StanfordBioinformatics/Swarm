package app.dao.client;

import com.google.api.gax.rpc.InvalidArgumentException;

import javax.annotation.Nullable;
import javax.validation.ValidationException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
//import static org.springframework.util.StringUtils.*;


public class StringUtils extends org.apache.commons.lang3.StringUtils {

    private static Character[] alphaNumericChars = null;

    public static String pathJoin(String a, String b) {
        if (!a.endsWith("/") && !b.startsWith("/")) {
            // neither have slash on join area
            return a + "/" + b;
        } else if (a.endsWith("/") && b.startsWith("/")) {
            // both have slash on join area, remove from b
            return a + b.substring(1);
        } else {
            // one has slash on join area, can just join
            return a + b;
        }
    }

    public static String ensureTrailingSlash(String s) {
        if (s.endsWith("/")) {
            return s;
        }
        return s + "/";
    }

    public static String ensureNoTrailingSlash(String s) {
        if (s == null) {
            return s;
        }
        if (s.endsWith("/")) {
            return s.substring(0, s.length() - 2);
        }
        return s;
    }

    public static String getLastNonEmptySegmentOfPath(String path) {
        path = ensureNoTrailingSlash(path);
        // filter out empty path segments like idx 1 in /path//to/something
        List<String> terms = new ArrayList<>();
        for (String t : path.split("/")) {
            if (!t.isEmpty()) {
                terms.add(t);
            }
        }
        return terms.get(terms.size() - 1);
    }

    public static String randomAlphaNumStringOfLength(int length) {
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        if (alphaNumericChars == null) {
            ArrayList<Character> chars = new ArrayList<Character>();
            for (char i = '0'; i <= '9'; i++) { chars.add(i); }
            for (char i = 'A'; i <= 'Z'; i++) { chars.add(i); }
            for (char i = 'a'; i <= 'z'; i++) { chars.add(i); }
            alphaNumericChars = new Character[chars.size()];
            alphaNumericChars = chars.toArray(alphaNumericChars);
        }
        for (int i = 0; i < length; i++) {
            char c = alphaNumericChars[random.nextInt(alphaNumericChars.length)];
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Quotes Athena database/table identifier string by period delimited parts.
     * Athena only allows this syntax for SELECT statements.
     * <br>
     * eg: database.tablename becomes "database"."tablename"
     * @param s input table identifier
     */
    public static String quoteAthenaTableIdentifier(String s) {
        if (s == null) { return null; }
        String[] terms = s.split("\\.");
        for (int i = 0; i < terms.length; i++) {
            // For regular queries in Athena, use double quotes for table name segments
            terms[i] = "\"" + terms[i] + "\"";
        }
        return String.join(".", terms);
    }
    public static String quoteAthenaTableIdentifierDDL(String s) {
        if (s == null) { return null; }
        String[] terms = s.split("\\.");
        for (int i = 0; i < terms.length; i++) {
            // For DDL queries in Athena, use backticks for identifiers
            terms[i] = "`" + terms[i] + "`";
        }
        return String.join(".", terms);
    }

    public static void disallowQuoteSemicolonSpace(String s) {
        if (s.contains("'")) {
            throw new ValidationException("String cannot contain single quotes: " + s);
        }
        if (s.contains(";")) {
            throw new ValidationException("String cannot contain semicolon: " + s);
        }
        if (s.matches(".*\\s.*")) {
            throw new ValidationException("String cannot contain whitespace: " + s);
        }
    }

    public boolean isNumeric(String s) {
        try {
            new BigInteger(s);
            return true;
        } catch (NumberFormatException e1) {
            try {
                new BigDecimal(s);
                return true;
            } catch (NumberFormatException e2) {
                return false;
            }
        }
    }

    @Nullable
    public static Long toLongNullable(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Nullable
    public static Double toDoubleNullable(String s) {
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static boolean listContainsIgnoreCase(List<String> list, String s) {
        for (String item : list) {
            if (item.equalsIgnoreCase(s)) {
                return true;
            }
        }
        return false;
    }

    public static String getSubstringAfter(String s, String delim) {
        int idx = s.indexOf(delim);
        if (idx > -1) {
            return s.substring(idx + delim.length());
        }
        return s;
    }

    /**
     * This method merges two lists of unique strings, while ignoring
     * the given prefixes on each string in any of the lists
     * @param list1 list 1 to merge with list 2
     * @param list2 list 2 to merge with list 1
     * @param list1Prefix list 1 prefix
     * @param list2Prefix list 2 prefix
     * @param aliasResults if true, appends to each column name in the result the name without the prefix
     * @return list of merged elements of list 1 and list 2, in order starting with the elements of list 1
     */
    public static List<String> mergeColumnListsIgnorePrefixes(
            List<String> list1, String list1Prefix,
            List<String> list2, String list2Prefix,
            boolean aliasResults) {
        System.out.printf("list1Prefix: %s, list2Prefix: %s\n", list1Prefix, list2Prefix);

        List<String> output = new ArrayList<>();
        List<String> outputWithoutPrefixes = new ArrayList<>();
        // in the case that one prefix is the prefix of another, take longest match.
        //prefixes.sort(Collections.reverseOrder());

        Set<String> set1 = new HashSet<>(); // ensure no duplicates in nlogn
        Set<String> set2 = new HashSet<>(); // ensure no duplicates in nlogn

        for (String s1 : list1) {
            set1.add(s1);
            String s1WithoutPrefix = s1;
            if (s1.startsWith(list1Prefix)) {
                s1WithoutPrefix = getSubstringAfter(s1, list1Prefix);
            }
            output.add(s1);
            outputWithoutPrefixes.add(s1WithoutPrefix);
        }
        if (set1.size() != list1.size()) {
            throw new IllegalArgumentException("list1 had duplicate elements");
        }

        for (String s2 : list2) {
            set2.add(s2);
            String s2WithoutPrefix = s2;
            if (s2.startsWith(list2Prefix)) {
                s2WithoutPrefix = getSubstringAfter(s2, list2Prefix);
            }
            if (!outputWithoutPrefixes.contains(s2WithoutPrefix)) {
                output.add(s2);
                outputWithoutPrefixes.add(s2WithoutPrefix);
            }
        }
        if (set2.size() != list2.size()) {
            throw new IllegalArgumentException("list2 had duplicate elements");
        }
        if (output.size() != outputWithoutPrefixes.size()) {
            throw new RuntimeException("Output size did not equal outputWithoutPrefixes size");
        }

        if (aliasResults) {
            for (int i = 0; i < output.size(); i++) {
                output.set(i, output.get(i) + " as " + outputWithoutPrefixes.get(i));
            }
        }
        return output;
    }
}
