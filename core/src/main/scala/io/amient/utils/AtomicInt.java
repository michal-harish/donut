/**
 * Copyright (C) 2015 Michal Harish
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.amient.utils;

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
