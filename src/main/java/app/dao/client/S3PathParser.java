package app.dao.client;

import javax.validation.ValidationException;
import java.util.Arrays;
import java.util.List;

public class S3PathParser {
    String bucket;
    String objectPath;

    S3PathParser(String url) {
        this("s3://", url);
    }

    S3PathParser(String protocol, String url) {
        url = url.trim();
        if (!url.startsWith(protocol)) {
            throw new ValidationException("Url does not start with '" + protocol + "': " + url);
        }
        // trim off protocol
        url = url.substring(protocol.length());

        int slashIndex = url.indexOf("/");
        if (slashIndex == -1 || (slashIndex == url.length() - 1)) {
            throw new ValidationException("Url does not contain an object path within the bucket");
        }

        // mybucket/objectname   slashIndex = 8
        bucket = url.substring(0, slashIndex);
        objectPath = url.substring(slashIndex + 1);
    }

    public List<String> getObjectPathList() {
        //ArrayList<String> ret = new ArrayList<>();
        List<String> ret = Arrays.asList(this.objectPath.split("/"));
        if (ret.get(ret.size() - 1).equals("")) {
            ret.remove(ret.size() - 1);
        }
        return ret;
    }

    public String getLastTerm() {
        List<String> l = getObjectPathList();
        return l.get(l.size() - 1);
    }

}
