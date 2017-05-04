package com.umantis.poc.exponentialbackoff;

/**
 * @author David Espinosa.
 */
public class RandomException extends Exception {

    public RandomException(final String message) {
        super(message);
    }
}
