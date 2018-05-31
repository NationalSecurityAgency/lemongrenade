package lemongrenade.core.util;

import org.apache.http.Header;

public class RequestResult {
    public int status_code;
    public String response_msg;
    public Header[] headers;

    RequestResult(int status_code, String response, Header[] headers) {
        this.status_code = status_code;
        this.response_msg = response;
        this.headers = headers;
    }

}
