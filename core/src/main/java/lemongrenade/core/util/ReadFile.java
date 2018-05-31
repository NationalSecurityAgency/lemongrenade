package lemongrenade.core.util;

import org.apache.commons.io.IOUtils;

import java.io.*;

public class ReadFile {
    public static String readFile(String file) throws IOException {
        InputStream is = ReadFile.class.getClassLoader().getResourceAsStream(file);
        if (null == is) {
            throw new IOException("Unable to read file:"+file);
        }
        String content = IOUtils.toString(is);
        is.close();
        return content;
    }

    public static String getContent(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder out = new StringBuilder();
        String line;
        while((line = reader.readLine()) != null) {
            out.append(line);
        }
        reader.close();
        return out.toString();
    }

    public static boolean isDirectory(String path) {
        File file = new File(path);
        if(file.exists() && file.isDirectory()) {
            return true;
        }
        return false;
    }

    public static boolean isFile(String path) {
        File file = new File(path);
        if(file.exists() && !file.isDirectory()) {
            return true;
        }
        return false;
    }
}