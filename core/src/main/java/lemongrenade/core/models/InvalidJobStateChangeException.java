package lemongrenade.core.models;

public class InvalidJobStateChangeException extends Exception {
    public InvalidJobStateChangeException(String message) {
        super(message);
    }
}