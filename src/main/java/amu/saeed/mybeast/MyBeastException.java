package amu.saeed.mybeast;

/**
 */
public class MyBeastException extends Exception {

    public MyBeastException(Throwable cause) {
        super(cause);
    }

    public MyBeastException(String message) {
        super(message);
    }

    public MyBeastException(String message, Throwable cause) {
        super(message, cause);
    }
}
