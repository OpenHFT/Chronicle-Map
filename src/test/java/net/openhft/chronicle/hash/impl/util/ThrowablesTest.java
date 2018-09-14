/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
