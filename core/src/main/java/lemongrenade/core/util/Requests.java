package lemongrenade.core.util;

/* This class assists with creating http(s) get/post requests. */

import lemongrenade.core.templates.LGAdapter;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.cookie.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.cookie.DefaultCookieSpec;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Iterator;

public class Requests {

    public static final int DEFAULT_TIMEOUT = 30000;
    public static Registry<CookieSpecProvider> r;
    public static CookieStore cookieStore;

    static {
        try {
            setCerts();
            r = RegistryBuilder.<CookieSpecProvider>create()
                    .register("easy", new EasySpecProvider())
                    .build();
            cookieStore = new BasicCookieStore();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // Put Requests
    public static RequestResult put(String url) throws Exception {
        return Requests.final_request(url, "", new JSONObject(), "PUT", DEFAULT_TIMEOUT, true);
    }
    public static RequestResult put(String url, JSONObject params) throws Exception {
        return Requests.final_request(url, params.toString(), new JSONObject(), "PUT", DEFAULT_TIMEOUT, true);
    }
    public static RequestResult put(String url, JSONObject params, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, params.toString(), requestParameters, "PUT", DEFAULT_TIMEOUT, true);
    }

    //Get Requests
    public static RequestResult get(String url) throws Exception {
        return Requests.final_request(url, "", new JSONObject(), "GET", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult get(String url, JSONObject params) throws Exception {
        return Requests.final_request(url, urlEncode(params), new JSONObject(), "GET", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult get(String url, JSONObject params, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, urlEncode(params), requestParameters, "GET", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult get(String url, JSONObject params, JSONObject requestParameters, int timeout) throws Exception {
        return Requests.final_request(url, urlEncode(params), requestParameters, "GET", timeout, true);
    }

    //Post Requests
    public static RequestResult post(String url, JSONObject params) throws Exception {
        return Requests.final_request(url, params.toString(), new JSONObject(), "POST", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult post(String url, JSONObject params, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, params.toString(), requestParameters, "POST", DEFAULT_TIMEOUT, true);
    }



    //JSONObject type data
    public static RequestResult post(String url, JSONObject params, JSONObject requestParameters, int timeout) throws Exception {
        return Requests.final_request(url, params.toString(), requestParameters, "POST", timeout, true);
    }

    public static RequestResult post(String url, JSONObject params, JSONObject requestParameters, Boolean urlEncode) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(params), requestParameters, "POST", DEFAULT_TIMEOUT, true);
        else
            return Requests.final_request(url, params.toString(), requestParameters, "POST", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult post(String url, JSONObject params, JSONObject requestParameters, Boolean urlEncode, int timeout) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(params), requestParameters, "POST", timeout, true);
        else
            return Requests.final_request(url, params.toString(), requestParameters, "POST", timeout, true);
    }

    //String type data
    public static RequestResult post(String url, String data, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, data, requestParameters, "POST", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult post(String url, String data, JSONObject requestParameters, Boolean urlEncode) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(data), requestParameters, "POST", DEFAULT_TIMEOUT, true);
        else
            return Requests.final_request(url, data, requestParameters, "POST", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult post(String url, String data, JSONObject requestParameters, Boolean urlEncode, int timeout) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(data), requestParameters, "POST", timeout, true);
        else
            return Requests.final_request(url, data, requestParameters, "POST", timeout, true);
    }

    //JSONArray type data
    public static RequestResult post(String url, JSONArray data, JSONObject requestParameters) throws Exception {
        return Requests.final_request(url, data.toString(), requestParameters, "POST", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult post(String url, JSONArray data, JSONObject requestParameters, Boolean urlEncode) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(data.toString()), requestParameters, "POST", DEFAULT_TIMEOUT, true);
        else
            return Requests.final_request(url, data.toString(), requestParameters, "POST", DEFAULT_TIMEOUT, true);
    }

    public static RequestResult post(String url, JSONArray data, JSONObject requestParameters, Boolean urlEncode, int timeout) throws Exception {
        if(urlEncode)
            return Requests.final_request(url, urlEncode(data.toString()), requestParameters, "POST", timeout, true);
        else
            return Requests.final_request(url, data.toString(), requestParameters, "POST", timeout, true);
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
            System.setProperty("jsse.enableSNIExtension", "false"); //Fixes unrecognized_name error on Adapters
        }
        catch(Exception e) {
            System.out.println("Error setting system properties for certificates. Ensure cert.json contains values for " +
                    "'cert_keystore', 'cert_keystore_type', 'cert_keystore_pass', 'cert_truststore', 'cert_truststore_type', and 'cert_truststore_pass'.");
            throw e;
        }
    }

    public static void add_headers(JSONObject headers, HttpGet httpObject) {
        Iterator<String> keys = headers.keys();
        String key;
        while(keys.hasNext()) {
            key = keys.next();
            httpObject.addHeader(key, headers.getString(key));
        }
    }

    public static void add_headers(JSONObject headers, HttpPost httpObject) {
        Iterator<String> keys = headers.keys();
        String key;
        while(keys.hasNext()) {
            key = keys.next();
            httpObject.addHeader(key, headers.getString(key));
        }
    }

    private static void add_headers(JSONObject headers, HttpPut httpObject) {
        Iterator<String> keys = headers.keys();
        String key;
        while(keys.hasNext()) {
            key = keys.next();
            httpObject.addHeader(key, headers.getString(key));
        }
    }

    public static HttpEntity getEntity(String params) throws UnsupportedEncodingException {//takes a string of params and creates an entity from them
        return new ByteArrayEntity(params.getBytes("UTF-8"));
    }



    static class EasyCookieSpec extends DefaultCookieSpec {
        @Override
        public void validate(Cookie arg0, CookieOrigin arg1) throws MalformedCookieException {
        //allow all cookies
        }
    }

    static class EasySpecProvider implements CookieSpecProvider {
        @Override
        public CookieSpec create(HttpContext context) {
            return new EasyCookieSpec();
        }
    }

    public static RequestResult final_request(String targetURL, String params, JSONObject headers, String method, int timeout, boolean verifySSL) throws Exception {
        CloseableHttpResponse response = null;
        CloseableHttpClient httpclient = null;
        try {
            // Create the client and issue the request
            RequestConfig requestConfig = RequestConfig.custom()
                    .setCookieSpec("easy")
                    .setConnectTimeout(timeout)
                    .setConnectionRequestTimeout(timeout)
                    .setSocketTimeout(timeout)
                    .build();
            HttpClientBuilder builder = HttpClientBuilder.create()
                    .setRedirectStrategy(new LaxRedirectStrategy()) //allow automatic redirects
                    .useSystemProperties()
                    .setDefaultRequestConfig(requestConfig)
                    .setDefaultCookieStore(cookieStore)
                    .setDefaultCookieSpecRegistry(r)
                    .setDefaultRequestConfig(requestConfig)
                    .disableAutomaticRetries();

            if(verifySSL == false) { //Don't verify host names, WARNING, only use this for testing and with caution
                builder.setSSLHostnameVerifier(new AllowAllHostnameVerifier());
            }

            httpclient = builder.build();

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
            else if (method.equals("POST")) { //POST method
                HttpPost httpPost = new HttpPost(url);
                add_headers(headers, httpPost);
                HttpEntity entity = getEntity(params);
                httpPost.setEntity(entity);
                response = httpclient.execute(httpPost);
            }
            else if(method.equals("PUT")) {
                HttpPut httpPut = new HttpPut(url);
                add_headers(headers, httpPut);
                HttpEntity entity = getEntity(params);
                httpPut.setEntity(entity);
                response = httpclient.execute(httpPut);
            }
            else {
                throw new Exception("Unknown method:"+method);
            }

            RequestResult result;
            Header[] response_headers = response.getAllHeaders();
            String response_msg = "";
            int status = response.getStatusLine().getStatusCode();
            if(status != 204) {
                HttpEntity entity = response.getEntity();
                response_msg = EntityUtils.toString(entity);

            }
            result = new RequestResult(status, response_msg, response_headers);
            // Close your connections when you're finished
            response.close();
            httpclient.close();
            return result;
        } catch (Exception e) {
            if(response != null) {
                response.close();
            }
            if(httpclient != null) {
                httpclient.close();
            }
            e.printStackTrace();
            throw e;
        }
    }

    //Converts a JSON key/value object into a url encoded string.
    public static String urlEncode(JSONObject input) throws Exception {
        String params = "";
        Iterator<String> keys = input.keys();
        String key;
        String value;
        if(keys.hasNext()) {
            key = keys.next();
            value = input.get(key).toString();
            params = key + "=" + URLEncoder.encode(value, "UTF-8");
        }
        while(keys.hasNext()) {
            key = keys.next();
            value = input.get(key).toString();
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
    public static JSONObject getDNs(String s_keystore, String pass, String type) {
        JSONObject DNs = new JSONObject();
        try {
            FileInputStream is = new FileInputStream(s_keystore);
            DNs = getDNs(is, pass, type);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return DNs;
    }

    public static JSONObject getDNs() throws FileNotFoundException {
        String file = LGAdapter.DEFAULT_CONFIG.get("cert_keystore").toString();
        String pass = LGAdapter.DEFAULT_CONFIG.get("cert_keystore_pass").toString();
        String type = LGAdapter.DEFAULT_CONFIG.get("cert_keystore_type").toString();
        InputStream is = new FileInputStream(file);
        return getDNs(is, pass, type);
    }

    public static JSONObject getDNs(InputStream is, String pass, String type) {
        JSONObject DNs = new JSONObject();
        try {
            KeyStore keystore = KeyStore.getInstance(type);
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