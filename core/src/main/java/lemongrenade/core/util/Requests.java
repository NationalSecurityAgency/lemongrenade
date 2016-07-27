package lemongrenade.core.util;

/* This class assists with creating http(s) get/post requests. */

import lemongrenade.core.templates.LGAdapter;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Iterator;

public class Requests {

    public static final int DEFAULT_TIMEOUT = 30000;

    static {
        try {
            setCerts();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    //cassl to get the target url with no params
    public static RequestResult get(String url) throws Exception {
        return Requests.final_request(url, "", new JSONObject(), "GET", DEFAULT_TIMEOUT);
    }

    public static RequestResult get(String url, JSONObject params) throws Exception {
        return Requests.final_request(url, urlEncode(params), new JSONObject(), "GET", DEFAULT_TIMEOUT);
    }

    public static RequestResult get(String url, JSONObject params, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, urlEncode(params), requestParameters, "GET", DEFAULT_TIMEOUT);
    }

    public static RequestResult get(String url, JSONObject params, JSONObject requestParameters, int timeout) throws Exception {
        return Requests.final_request(url, urlEncode(params), requestParameters, "GET", timeout);
    }

    public static RequestResult post(String url, JSONObject params) throws Exception {
        return Requests.final_request(url, urlEncode(params), new JSONObject(), "POST", DEFAULT_TIMEOUT);
    }

    public static RequestResult post(String url, JSONObject params, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, urlEncode(params), requestParameters, "POST", DEFAULT_TIMEOUT);
    }

    public static RequestResult post(String url, String data, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, urlEncode(data), requestParameters, "POST", DEFAULT_TIMEOUT);
    }

    public static RequestResult post(String url, String data, JSONObject requestParameters, Boolean urlEncode) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(data), requestParameters, "POST", DEFAULT_TIMEOUT);
        else
            return Requests.final_request(url, data, requestParameters, "POST", DEFAULT_TIMEOUT);
    }

    public static RequestResult post(String url, String data, JSONObject requestParameters, Boolean urlEncode, int timeout) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(data), requestParameters, "POST", timeout);
        else
            return Requests.final_request(url, data, requestParameters, "POST", timeout);
    }

    //sets the keystore and truststore to values set in the default config - run this when defaults aren't good
    public static void setCerts () {
        try {
            System.setProperty("javax.net.ssl.keyStore", LGAdapter.DEFAULT_CONFIG.get("cert_keystore").toString());
            System.setProperty("javax.net.ssl.keyStoreType", LGAdapter.DEFAULT_CONFIG.get("cert_keystore_type").toString());
            System.setProperty("javax.net.ssl.keyStorePassword", LGAdapter.DEFAULT_CONFIG.get("cert_keystore_pass").toString());
            System.setProperty("javax.net.ssl.trustStore", LGAdapter.DEFAULT_CONFIG.get("cert_truststore").toString());
            System.setProperty("javax.net.ssl.trustStoreType", LGAdapter.DEFAULT_CONFIG.get("cert_truststore_type").toString());
            System.setProperty("javax.net.ssl.trustStorePassword", LGAdapter.DEFAULT_CONFIG.get("cert_truststore_pass").toString());
        }
        catch(Exception e) {
            System.out.println("Error setting system properties for certificates. Ensure cert.json contains values for " +
                    "'cert_keystore', 'cert_keystore_type', 'cert_keystore_pass', 'cert_truststore', 'cert_truststore_type', and 'cert_truststore_pass'.");
            throw e;
        }
    }

    public static void add_headers(JSONObject headers, HttpGet httpObject) {
        Iterator<String> keys = headers.keys();
        String key = null;
        while(keys.hasNext()) {
            key = keys.next();
            httpObject.addHeader(key, headers.getString(key));
        }
    }

    public static void add_headers(JSONObject headers, HttpPost httpObject) {
        Iterator<String> keys = headers.keys();
        String key = null;
        while(keys.hasNext()) {
            key = keys.next();
            httpObject.addHeader(key, headers.getString(key));
        }
    }

    public static RequestResult final_request(String targetURL, String params, JSONObject headers, String method, int timeout) throws Exception {
        try
        {
        // Create the client and issue the request
        CloseableHttpResponse response;
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setSocketTimeout(timeout)
                .build();

        CloseableHttpClient httpclient = HttpClientBuilder.create().useSystemProperties().setDefaultRequestConfig(requestConfig).build();
        String url;
        method = method.toUpperCase();
        url = targetURL;
        if(method.equals("GET")) {
            if(params.length() > 0)
                url = url + "?" + params;
            HttpGet httpGet = new HttpGet(url);
            add_headers(headers, httpGet);
            response = httpclient.execute(httpGet);
        }
        else { //POST method
            HttpPost httpPost = new HttpPost(url);
            add_headers(headers, httpPost);
            HttpEntity entity = new StringEntity(params);
            httpPost.setEntity(entity);
            response = httpclient.execute(httpPost);
        }

        HttpEntity entity = response.getEntity();
        String response_msg = EntityUtils.toString(entity);
        RequestResult result = new RequestResult(response.getStatusLine().getStatusCode(), response_msg);
        // Close your connections when you're finished
        response.close();
        httpclient.close();
        return result;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    //Converts a JSON key/value object into a url encoded string.
    public static String urlEncode(JSONObject input) throws Exception {
        String params = "";
        Iterator<String> keys = input.keys();
        String key = null;
        String value = null;
        if(keys.hasNext()) {
            key = keys.next();
            value = input.get(key).toString();
            params = key + "=" + URLEncoder.encode(value, "UTF-8");
        }
        while(keys.hasNext()) {
            key = keys.next();
            value = input.getString(key);
            params += "&" + key + "=" + URLEncoder.encode(value, "UTF-8");
        }
        return params;
    }

    public static String urlEncode(String input) throws Exception {
        return URLEncoder.encode(input, "UTF-8");
    }

    /* Create a keystore from a .p12/.pfx
    keytool -v -importkeystore -srckeystore my_key.p12 -srcstoretype PKCS12 -destkeystore new_truststore.jks -deststoretype JKS
    IMPORTANT: The password on the new trust store must match the password for the .p12 key used */
    //Change keystore password: keytool -storepasswd -keystore my.keystore
    //Change key password: keytool -keypasswd -alias <key_name> -keystore my.keystore

    //example from http://www.java2s.com/Tutorial/Java/0490__Security/GettingtheSubjectandIssuerDistinguishedNamesofanX509Certificate.htm
    public static JSONObject getDNs(String s_keystore, String pass) {
        JSONObject DNs = new JSONObject();
        try {
            FileInputStream is = new FileInputStream(s_keystore);
            DNs = getDNs(is, pass);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return DNs;
    }

    public static JSONObject getDNs(InputStream is, String pass) {
        JSONObject DNs = new JSONObject();
        try {
            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            keystore.load(is, pass.toCharArray());
            Enumeration e = keystore.aliases();
            for(; e.hasMoreElements();) {
                String alias = (String) e.nextElement();
                java.security.cert.Certificate cert = keystore.getCertificate(alias);
                if(cert instanceof X509Certificate) {
                    X509Certificate x509cert = (X509Certificate) cert;
                    Principal principal = x509cert.getSubjectDN();
                    DNs.put("user_dn", principal.getName());
                    principal = x509cert.getIssuerDN();
                    DNs.put("issuer_dn", principal.getName());
                    break;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return DNs;
    }

    //Enables full debug output for requests
    public static void enableFullDebug() {
        System.out.println("'javax.net.debug' set to 'all'.");
        System.setProperty("javax.net.debug", "all");
    }
}