package lemongrenade.core.coordinator;

/**
 * Job Failure Exceptions are thrown when there is a fatal error with job processing.
 */
public class JobFailureException extends Exception {
    public JobFailureException(String message) {
        super(message);
    }
}
