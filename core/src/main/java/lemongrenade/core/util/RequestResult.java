package lemongrenade.core.util;

public class RequestResult {
    public int status_code;
    public String response_msg;

    RequestResult(int status_code, String response) {
        this.status_code = status_code;
        this.response_msg = response;
    }
}
