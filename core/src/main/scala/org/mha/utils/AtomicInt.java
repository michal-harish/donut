package org.mha.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mharis on 07/10/15.
 */
public class AtomicInt {
    private final AtomicInteger i;

    public AtomicInt(AtomicInteger i) {
        this.i = i;
    }

    public void setIfGreater(int newValue) {
        boolean success;
        do {
            int currentValue = i.get();
            if (newValue > currentValue) {
                success = i.compareAndSet(currentValue, newValue);
            } else {
                return;
            }
        } while(!success);

    }


}
