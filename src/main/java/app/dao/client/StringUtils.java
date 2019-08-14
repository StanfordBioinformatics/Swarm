package app.dao.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class StringUtils {

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
}
