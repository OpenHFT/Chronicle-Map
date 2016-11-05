/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.impl.util;

import org.junit.Test;

import java.io.IOException;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ThrowablesTest {

    @Test(expected = NullPointerException.class)
    public void testPropagateNull() {
        Throwables.propagate(null);
    }

    @Test(expected = AssertionError.class)
    public void testPropagateError() {
        Throwables.propagate(new AssertionError());
    }

    @Test(expected = IllegalStateException.class)
    public void testPropagateUncheckedException() {
        Throwables.propagate(new IllegalStateException());
    }

    @Test(expected = RuntimeException.class)
    public void testPropagateCheckedException() {
        Throwables.propagate(new IOException());
    }

    @Test(expected = NullPointerException.class)
    public void testPropagateNotWrappingNull() throws IOException {
        Throwables.propagateNotWrapping(null, IOException.class);
    }

    @Test(expected = NullPointerException.class)
    public void testPropagateNotWrappingNullWrappingType() throws Throwable {
        Throwables.propagateNotWrapping(new IOException(), null);
    }

    @Test(expected = AssertionError.class)
    public void testPropagateNotWrappingError() throws IOException {
        Throwables.propagateNotWrapping(new AssertionError(), IOException.class);
    }

    @Test(expected = IllegalStateException.class)
    public void testPropagateNotWrappingUncheckedException() throws IOException {
        Throwables.propagateNotWrapping(new IllegalStateException(), IOException.class);
    }

    @Test(expected = IOException.class)
    public void testPropagateNotWrappingNotWrappingEException() throws IOException {
        Throwables.propagateNotWrapping(new IOException(), IOException.class);
    }

    @Test(expected = RuntimeException.class)
    public void testPropagateNotWrappingCheckedException() throws IOException {
        Throwables.propagateNotWrapping(new Exception(), IOException.class);
    }
}
