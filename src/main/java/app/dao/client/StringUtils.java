package app.dao.client;

import javax.annotation.Nullable;
import javax.validation.ValidationException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
            terms[i] = "\"" + terms[i] + "\"";
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

}
