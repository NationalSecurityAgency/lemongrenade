package lemongrenade.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class LGProperties {
    private static final Logger log = LoggerFactory.getLogger(LGProperties.class);
    private static Properties prop = new Properties();
    final private static String DEFAULT_PATH = "/opt/lemongrenade/conf";
    final private static String FILENAME = "lemongrenade.props";
    final private static String CONFIG_PATH = getConfigPath();

    //When LGProperties is initialized, determine the correct 'lemongrenade.props', get the stream, and load the properties
    static {
        try {
            InputStream is = getStream(FILENAME);
            loadPropertiesStream(is);
        } catch (FileNotFoundException e) {
            log.error("Unable to read file:"+FILENAME);
            e.printStackTrace();
        }
    }

    /**
     * Reads an input file from the first location in the following priority:
     * 1 location defined by env LEMONGRENADE_CONFIGS
     * 2 '/opt/lemongrenade/conf/'
     * 3 Class resource
     * @param fileName String for name of file to read in.
     * @return Returns an input stream
     * @throws FileNotFoundException Thrown if the file couldn't be read
     */
    public static InputStream getStream(String fileName) throws FileNotFoundException {
        InputStream is;
        if(!ReadFile.isDirectory(CONFIG_PATH)) {
            log.warn("'"+CONFIG_PATH+"' is not a directory. Defaulting to '"+fileName+"' JAR resource.");
            is = LGProperties.class.getClassLoader().getResourceAsStream(fileName);
        }
        else {
            String file = CONFIG_PATH + "/" + fileName;
            if (ReadFile.isFile(file)) {
                log.info("Reading file '"+file+"'.");
                is = new FileInputStream(file);
            } else {
                log.info("Couldn't read file '" + file + "' at '" + CONFIG_PATH + "'. Defaulting to '"+fileName+"' JAR resource.");
                is = LGProperties.class.getClassLoader().getResourceAsStream(fileName);
            }
        }
        if(is == null) {
            throw new FileNotFoundException("Unable to read file:"+fileName);
        }
        return is;
    }

    //Returns environment variable 'LEMONGRENADE_CONFIGS', or default '/opt/lemongrenade/conf' if undefined.
    public static String getConfigPath() {
        String config_path = System.getenv("LEMONGRENADE_CONFIGS");
        if(config_path == null) {
            config_path = DEFAULT_PATH;
            log.info("Checking default path:"+DEFAULT_PATH+" for '"+FILENAME+"'.");
        }
        else {
            log.info("Environment variable 'LEMONGRENADE_CONFIGS' set to '" + config_path + "'.");
        }
        return config_path;
    }

    public static void loadPropertiesStream(InputStream stream) {
        try {
            prop.load(stream);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /** */
    public static Boolean has(String _key) {
        return prop.containsKey(_key);
    }

    /** */
    public static String get(String _key) {
        return prop.getProperty(_key);
    }

    /** */
    public static String get(String _key, String defaultValue) {
        return prop.getProperty(_key, defaultValue);
    }

    /** */
    public static int getInteger(String _key, int defaultValue) {
        try  {
            String str = prop.getProperty(_key);
            if ((str == null) || str.equals("")) return defaultValue;
            int propertyIntValue = Integer.parseInt(str);
            return propertyIntValue;
        }
        catch (NumberFormatException e)  {
            System.out.println("[LGProperites]  Number format exception for key:"+_key+" default:"+defaultValue);

        }
        return defaultValue;
    }

    public static void main(String[] args) {
        String path = LGProperties.get("lemongraph_url");
        System.out.println("lemongraph_url = "+path);
    }

}
