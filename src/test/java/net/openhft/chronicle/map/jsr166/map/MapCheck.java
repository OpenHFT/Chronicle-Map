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

package net.openhft.chronicle.map.jsr166.map;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.*;
import java.util.*;

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/**
 * @test
 * @synopsis Times and checks basic map operations
 * <p>
 * When run with "s" second arg, this requires file "testwords", which
 * is best used with real words.  We can't check in this file, but you
 * can create one from a real dictionary (1 line per word) and then run
 * linux "shuf" to randomize entries.
 */

public class MapCheck {
    static final String MISSING = "MISSING";
    static final LoopHelpers.SimpleRandom srng = new LoopHelpers.SimpleRandom();
    static final Random rng = new Random(3152688);
    static TestTimer timer = new TestTimer();
    static Class eclass;
    static volatile int checkSum;
    static int counter = 0;

    static void reallyAssert(boolean b) {
        if (!b) throw new Error("Failed Assertion");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        int numTests = 20;
        int size = 36864; // about midway of HashMap resize interval

        if (args.length == 0)
            System.out.println("Usage: MapCheck mapclass [int|float|string|object] [trials] [size] [serialtest]");

        if (args.length > 1) {
            String et = args[1].toLowerCase();
            if (et.startsWith("i"))
                eclass = java.lang.Integer.class;
            else if (et.startsWith("f"))
                eclass = java.lang.Float.class;
            else if (et.startsWith("s"))
                eclass = java.lang.String.class;
            else if (et.startsWith("d"))
                eclass = java.lang.Double.class;
        }
        if (eclass == null)
            eclass = Integer.class;

        if (args.length > 2)
            numTests = Integer.parseInt(args[2]);

        if (args.length > 3)
            size = Integer.parseInt(args[3]);

        boolean doSerializeTest = args.length > 4;

        while ((size & 3) != 0) ++size;

        System.out.print(" elements: " + eclass.getName());
        System.out.print(" trials: " + numTests);
        System.out.print(" size: " + size);
        System.out.println();

        Object[] key = new Object[size];
        Object[] absent = new Object[size];
        initializeKeys(key, absent, size);

        precheck(size, key, absent);

        for (int rep = 0; rep < numTests; ++rep) {
            mainTest(key, absent);
            if ((rep & 3) == 3 && rep < numTests - 1) {
                shuffle(key);
                //                Jvm.pause(10);
            }
        }

        TestTimer.printStats();

        checkNullKey();

        if (doSerializeTest)
            serTest(size);
    }

    static Map newMap() {
        try {
            return ChronicleMapBuilder.of(Object.class, Object.class).create();
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate CHM : " + e);
        }
    }

    static void closeMap(Map map) {
        if (map instanceof ChronicleMap) {
            ChronicleMap chm = (ChronicleMap) map;
            try {
                chm.close();
            } catch (Exception e) {
                throw new RuntimeException("Can't close CHM : " + e);
            }
        }
    }

    static void precheck(int n, Object[] key, Object[] abs) {
        int ck = 0;
        Map s = newMap();
        for (int i = 0; i < n; i++) {
            Object k = key[i];
            if (k == null) throw new Error("Null key at" + i);
            ck += System.identityHashCode(k);
            if (i == 409)
                Thread.yield();
            Object v = s.put(k, k);
            if (v != null)
                throw new Error("Duplicate " + k + " / " + v);
        }
        for (int i = 0; i < n; i++) {
            Object k = abs[i];
            if (k == null) throw new Error("Null key at" + i);
            ck += System.identityHashCode(k);
            Object v = s.put(k, k);
            if (v != null)
                throw new Error("Duplicate " + k + " / " + v);
        }
        checkSum += ck;
        closeMap(s);
    }

    static void checkNullKey() {
        Map m = newMap();
        Object x = (byte) 1;
        Object v;
        try {
            m.put(null, x);
            v = m.get(null);
        } catch (NullPointerException npe) {
            System.out.println("Map does not allow null keys");
            return;
        } catch (IllegalArgumentException npe) {
            System.out.println("Map does not allow null keys");
            return;
        }
        if (!v.equals(x)) throw new Error();
        if (m.remove(null) != v) throw new Error();
        if (m.get(null) != null) throw new Error();
        closeMap(m);
    }

    static void getTest(String nm, int n, Map s, Object[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            Object v = s.get(key[i]);
            if (v != null && v.getClass() == eclass)
                ++sum;
        }
        timer.finish();
        reallyAssert(sum == expect);
        checkSum += sum;
    }

    // unused
    static void getTestBoxed(String nm, int n, Map s, Object[] key, int expect) {
        int sum = 0;
        Map<Integer, Integer> intMap = (Map<Integer, Integer>) s;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (intMap.get(i) != i) ++sum;
        }
        timer.finish();
        reallyAssert(sum == expect);
    }

    static void remTest(String nm, int n, Map s, Object[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.remove(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert(sum == expect);
        checkSum += sum;
    }

    static void clrTest(int n, Map s) {
        String nm = "Remove Present         ";
        timer.start(nm, n);
        s.clear();
        timer.finish();
        reallyAssert(s.isEmpty());
    }

    static void putTest(String nm, int n, Map s, Object[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            Object k = key[i];
            Object v = s.put(k, k);
            if (v == null) ++sum;
        }
        timer.finish();
        reallyAssert(sum == expect);
        checkSum += sum;
    }

    static void keyTest(String nm, int n, Map s, Object[] key, int expect) {
        int sum = 0;
        timer.start(nm, n);
        for (int i = 0; i < n; i++) {
            if (s.containsKey(key[i])) ++sum;
        }
        timer.finish();
        reallyAssert(sum == expect);
        checkSum += sum;
    }

    // version without timing for uncategorized tests
    static void untimedKeyTest(String nm, int n, Map s, Object[] key, int expect) {
        int sum = 0;
        for (int i = 0; i < n; i++) {
            if (s.containsKey(key[i])) ++sum;
        }
        reallyAssert(sum == expect);
        checkSum += sum;
    }

    static void remHalfTest(String nm, int n, Map s, Object[] key, int expect) {
        int sum = 0;
        timer.start(nm, n / 2);
        for (int i = n - 2; i >= 0; i -= 2) {
            if (s.remove(key[i]) != null) ++sum;
        }
        timer.finish();
        reallyAssert(sum == expect);
        checkSum += sum;
    }

    static void valTest(Map s, Object[] key) {
        int size = s.size();
        int sum = 0;
        timer.start("Traverse key or value  ", size);
        if (s.containsValue(MISSING)) ++sum;
        timer.finish();
        reallyAssert(sum == 0);
        checkSum += sum;
    }

    static Object kitTest(Map s, int size) {
        Object last = null;
        int sum = 0;
        timer.start("Traverse key or value  ", size);
        for (Iterator it = s.keySet().iterator(); it.hasNext(); ) {
            Object x = it.next();
            if (x != last && x != null && x.getClass() == eclass)
                ++sum;
            last = x;
        }
        timer.finish();
        reallyAssert(sum == size);
        checkSum += sum;
        return last;
    }

    static Object vitTest(Map s, int size) {
        Object last = null;
        int sum = 0;
        timer.start("Traverse key or value  ", size);
        for (Iterator it = s.values().iterator(); it.hasNext(); ) {
            Object x = it.next();
            if (x != last && x != null && x.getClass() == eclass)
                ++sum;
            last = x;
        }
        timer.finish();
        reallyAssert(sum == size);
        checkSum += sum;
        return last;
    }

    static void eitTest(Map s, int size) {
        int sum = 0;
        timer.start("Traverse entry         ", size);
        for (Iterator it = s.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry e = (Map.Entry) it.next();
            Object k = e.getKey();
            Object v = e.getValue();
            if (k != null && k.getClass() == eclass &&
                    v != null && v.getClass() == eclass)
                ++sum;
        }
        timer.finish();
        reallyAssert(sum == size);
        checkSum += sum;
    }

    static void itRemTest(Map s, int size) {
        int sz = s.size();
        reallyAssert(sz == size);
        timer.start("Remove Present         ", size);
        int sum = 0;
        for (Iterator it = s.keySet().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
            ++sum;
        }
        timer.finish();
        reallyAssert(sum == sz);
        checkSum += sum;
    }

    static void itHalfRemTest(Map s, int size) {
        int sz = s.size();
        reallyAssert(sz == size);
        timer.start("Remove Present         ", size);
        int sum = 0;
        for (Iterator it = s.keySet().iterator(); it.hasNext(); ) {
            it.next();
            it.remove();
            if (it.hasNext())
                it.next();
            ++sum;
        }
        timer.finish();
        reallyAssert(sum == sz / 2);
        checkSum += sum;
    }

    static void putAllTest(String nm, int n, Map src, Map dst) {
        timer.start(nm, n);
        dst.putAll(src);
        timer.finish();
        reallyAssert(src.size() == dst.size());
    }

    static void serTest(int size) throws IOException, ClassNotFoundException {
        Map s = newMap();
        if (!(s instanceof Serializable))
            return;
        System.out.print("Serialize              : ");

        for (int i = 0; i < size; i++) {
            s.put(new Integer(i), Boolean.TRUE);
        }

        long startTime = System.currentTimeMillis();

        FileOutputStream fs = new FileOutputStream("MapCheck.dat");
        ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(fs));
        out.writeObject(s);
        out.close();

        FileInputStream is = new FileInputStream("MapCheck.dat");
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(is));
        Map m = (Map) in.readObject();

        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;

        System.out.print(time + "ms");

        if (s instanceof IdentityHashMap) return;
        reallyAssert(s.equals(m));
        closeMap(s);
    }

    static void mainTest(Object[] key, Object[] absent) {
        Map s = newMap();
        int size = key.length;

        putTest("Add    Absent          ", size, s, key, size);
        reallyAssert(s.size() == size);
        getTest("Access Present         ", size, s, key, size);
        getTest("Search Absent          ", size, s, absent, 0);
        kitTest(s, size);
        vitTest(s, size);
        eitTest(s, size);
        putTest("Modify Present         ", size, s, key, 0);
        reallyAssert(s.size() == size);
        untimedKeyTest("Access Present         ", size, s, key, size);
        keyTest("Search Absent          ", size, s, absent, 0);
// No supported:        valTest(s, key);
        remTest("Search Absent          ", size, s, absent, 0);
        reallyAssert(s.size() == size);
        remHalfTest("Remove Present         ", size, s, key, size / 2);
        reallyAssert(s.size() == size / 2);
        getTest("Access Present         ", size, s, key, size / 2);
        putTest("Add    Absent          ", size, s, key, size / 2);
        reallyAssert(s.size() == size);
        getTest("Access Present         ", size, s, key, size);
        getTest("Search Absent          ", size, s, absent, 0);
        itRemTest(s, size);
        s.clear();

        putTest("Add    Absent          ", size, s, key, size);
        reallyAssert(s.size() == size);
        getTest("Access Present         ", size, s, key, size);
        untimedKeyTest("Access Present         ", size, s, key, size);
        kitTest(s, size);
        vitTest(s, size);
        eitTest(s, size);
        twoMapTest1(s, key, absent);
        twoMapTest2(s, key, absent);
        closeMap(s);
    }

    static void twoMapTest1(Map s, Object[] key, Object[] absent) {
        int size = s.size();
        Map s2 = newMap();
        putAllTest("Add    Absent          ", size, s, s2);
        getTest("Access Present         ", size, s2, key, size);
        itHalfRemTest(s2, size);
        reallyAssert(s2.size() == size / 2);
        itHalfRemTest(s2, size / 2);
        reallyAssert(s2.size() == size / 4);
        putTest("Add    Absent          ", size, s2, absent, size);
        putTest("Add    Absent          ", size, s2, key, size * 3 / 4);
        reallyAssert(s2.size() == size * 2);
        clrTest(size, s2);
        closeMap(s2);
    }

    static void twoMapTest2(Map s, Object[] key, Object[] absent) {
        int size = key.length;

        Map s2 = newMap();
        putAllTest("Add    Absent          ", size, s, s2);
        putAllTest("Modify Present         ", size, s, s2);

        Object lastkey = kitTest(s2, size);
        Object hold = s2.get(lastkey);
        int sum = 0;

        timer.start("Traverse entry         ", size * 12); // 12 until finish

        int sh1 = s.hashCode() - s2.hashCode();
        reallyAssert(sh1 == 0);
        boolean eq1 = s2.equals(s);
        boolean eq2 = s.equals(s2);
        reallyAssert(eq1 && eq2);

        Set es2 = s2.entrySet();
        for (Iterator it = s.entrySet().iterator(); it.hasNext(); ) {
            Object entry = it.next();
            if (es2.contains(entry)) ++sum;
        }
        reallyAssert(sum == size);

        s2.put(lastkey, MISSING);

        int sh2 = s.hashCode() - s2.hashCode();
        reallyAssert(sh2 != 0);

        eq1 = s2.equals(s);
        eq2 = s.equals(s2);
        reallyAssert(!eq1 && !eq2);

        sum = 0;
        for (Iterator it = s.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry e = (Map.Entry) it.next();
            e.setValue(absent[sum++]);
        }
        reallyAssert(sum == size);
        for (Iterator it = s2.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry e = (Map.Entry) it.next();
            e.setValue(s.get(e.getKey()));
        }

        timer.finish();

        int rmiss = 0;
        timer.start("Remove Present         ", size * 2);
        Iterator s2i = s2.entrySet().iterator();
        Set es = s.entrySet();
        while (s2i.hasNext()) {
            if (!es.remove(s2i.next()))
                ++rmiss;
        }
        timer.finish();
        reallyAssert(rmiss == 0);

        clrTest(size, s2);
        reallyAssert(s2.isEmpty() && s.isEmpty());
        closeMap(s2);
    }

    static void itTest4(Map s, int size, int pos) {
        IdentityHashMap seen = new IdentityHashMap(size);
        reallyAssert(s.size() == size);
        int sum = 0;
        timer.start("Iter XEntry            ", size);
        Iterator it = s.entrySet().iterator();
        Object k = null;
        Object v = null;
        for (int i = 0; i < size - pos; ++i) {
            Map.Entry x = (Map.Entry) (it.next());
            k = x.getKey();
            v = x.getValue();
            seen.put(k, k);
            if (!x.equals(MISSING))
                ++sum;
        }
        reallyAssert(s.containsKey(k));
        it.remove();
        reallyAssert(!s.containsKey(k));
        while (it.hasNext()) {
            Map.Entry x = (Map.Entry) (it.next());
            Object k2 = x.getKey();
            seen.put(k2, k2);
            if (!x.equals(MISSING))
                ++sum;
        }

        reallyAssert(s.size() == size - 1);
        s.put(k, v);
        reallyAssert(seen.size() == size);
        timer.finish();
        reallyAssert(sum == size);
        reallyAssert(s.size() == size);
    }

    static void initializeKeys(Object[] key, Object[] absent, int size) {
        // Object cannot be used as it cannot be serialized.
        if (eclass == Object.class || eclass == Integer.class) {
            initInts(key, absent, size);
        } else if (eclass == Float.class) {
            initFloats(key, absent, size);
        } else if (eclass == Double.class) {
            initDoubles(key, absent, size);
        } else if (eclass == String.class) {
            initWords(size, key, absent);
        } else
            throw new Error("unknown type");
    }

    static void initInts(Object[] key, Object[] absent, int size) {
        for (int i = 0; i < size; ++i)
            key[i] = Integer.valueOf(i);
        Map m = newMap();
        int k = 0;
        while (k < size) {
            int r = srng.next();
            if (r < 0 || r >= size) {
                Integer ir = Integer.valueOf(r);
                if (m.put(ir, ir) == null)
                    absent[k++] = ir;
            }
        }
        closeMap(m);
    }

    static void initFloats(Object[] key, Object[] absent, int size) {
        Map m = newMap();
        for (int i = 0; i < size; ++i) {
            float r = (float) i;
            key[i] = r;
            m.put(r, r);
        }
        int k = 0;
        while (k < size) {
            Float ir = rng.nextFloat();
            if (m.put(ir, ir) == null)
                absent[k++] = ir;
        }
        closeMap(m);
    }

    static void initDoubles(Object[] key, Object[] absent, int size) {
        Map m = newMap();
        for (int i = 0; i < size; ++i) {
            double r = (double) i;
            key[i] = r;
            m.put(r, r);
        }
        int k = 0;
        while (k < size) {
            Double ir = rng.nextDouble();
            if (m.put(ir, ir) == null)
                absent[k++] = ir;
        }
        closeMap(m);
    }

    // Use as many real words as possible, then use fake random words

    static void initWords(int size, Object[] key, Object[] abs) {
        String fileName = "testwords.txt";
        int ki = 0;
        int ai = 0;
        try {
            FileInputStream fr = new FileInputStream(fileName);
            BufferedInputStream in = new BufferedInputStream(fr);
            while (ki < size || ai < size) {
                StringBuilder sb = new StringBuilder();
                for (; ; ) {
                    int c = in.read();
                    if (c < 0) {
                        if (ki < size)
                            randomWords(key, ki, size);
                        if (ai < size)
                            randomWords(abs, ai, size);
                        in.close();
                        return;
                    }
                    if (c == '\n') {
                        String s = sb.toString();
                        if (ki < size)
                            key[ki++] = s;
                        else
                            abs[ai++] = s;
                        break;
                    }
                    sb.append((char) c);
                }
            }
            in.close();
        } catch (IOException ex) {
            System.out.println("Can't read words file:" + ex);
            throw new Error(ex);
        }
    }

    static void randomWords(Object[] ws, int origin, int size) {
        for (int i = origin; i < size; ++i) {
            int k = 0;
            int len = 2 + (srng.next() & 0xf);
            char[] c = new char[len * 4 + 1];
            for (int j = 1; j < len; ++j) {
                int r = srng.next();
                c[k++] = (char) (' ' + (r & 0x7f));
                r >>>= 8;
                c[k++] = (char) (' ' + (r & 0x7f));
                r >>>= 8;
                c[k++] = (char) (' ' + (r & 0x7f));
                r >>>= 8;
                c[k++] = (char) (' ' + (r & 0x7f));
            }
            c[k++] = (char) ((i & 31) | 1); // never == to any testword
            ws[i] = new String(c);
        }
    }

    static void shuffle(Object[] keys) {
        int size = keys.length;
        for (int i = size; i > 1; i--) {
            int r = rng.nextInt(i);
            Object t = keys[i - 1];
            keys[i - 1] = keys[r];
            keys[r] = t;
        }
    }

    static void shuffle(ArrayList keys) {
        int size = keys.size();
        for (int i = size; i > 1; i--) {
            int r = rng.nextInt(i);
            Object t = keys.get(i - 1);
            keys.set(i - 1, keys.get(r));
            keys.set(r, t);
        }
    }

    static final class TestTimer {
        static final java.util.TreeMap accum = new java.util.TreeMap();
        private String name;
        private long numOps;
        private long startTime;

        static void printStats() {
            for (Iterator it = accum.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry e = (Map.Entry) it.next();
                Stats stats = (Stats) e.getValue();
                System.out.print(e.getKey() + ": ");
                long s;
                long n = stats.number;
                if (n == 0) {
                    n = stats.firstn;
                    s = stats.first;
                } else
                    s = stats.sum;

                double t = ((double) s) / n;
                long nano = Math.round(t);
                System.out.printf("%6d", +nano);
                System.out.println();
            }
        }

        void start(String name, long numOps) {
            this.name = name;
            this.numOps = numOps;
            startTime = System.nanoTime();
        }

        void finish() {
            long elapsed = System.nanoTime() - startTime;
            Object st = accum.get(name);
            if (st == null)
                accum.put(name, new Stats(elapsed, numOps));
            else
                ((Stats) st).addTime(elapsed, numOps);
        }
    }

    static final class Stats {
        long sum;
        long number;
        long first;
        long firstn;

        Stats(long t, long n) {
            first = t;
            firstn = n;
        }

        void addTime(long t, long n) {
            sum += t;
            number += n;
        }
    }

}
