package lemongrenade.core.util;

import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;

public class ReadFile {
    public static String readFile(String file) throws IOException {
//        String content = new String(Files.readAllBytes(Paths.get(file)));
        InputStream is = ReadFile.class.getClassLoader().getResourceAsStream(file);
        // TODO: this needs a better error condition handler
        if (null == is) { return "{}"; }
        String content = IOUtils.toString(is);
        is.close();
        return content;
    }
}