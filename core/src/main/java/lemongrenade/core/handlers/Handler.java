package lemongrenade.core.handlers;

//Extend this interface to create handlers for T data types.
public interface Handler<T> {
    void handle(T input) throws Exception;
}
