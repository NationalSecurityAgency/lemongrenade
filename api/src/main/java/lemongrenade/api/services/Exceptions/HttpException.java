package lemongrenade.api.services.Exceptions;

/**
 *
 */
public class HttpException extends org.apache.http.HttpException {
    private Integer status_code = null;

    public HttpException(String message, Integer status_code) {
        super(message);
        this.status_code = status_code;
    }

    public Integer getStatus() {
        return status_code;
    }
}
