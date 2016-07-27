package lemongrenade.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class LGProperties {

    private static Properties prop = new Properties();
    private static InputStream input = null;

    static {
        try {
            input = LGProperties.class.getClassLoader().getResourceAsStream("lemongrenade.props");
            if(input==null){
                System.out.println("[LGProperites] ERROR Unable to find lemongrenade.props in classpath");
            }
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
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


    /** move to junit
     *
     */
    public static void main(String[] args) {
        String path = LGProperties.get("lemongraph_url");
        System.out.println("lemongraph_url = "+path);

    }

}
