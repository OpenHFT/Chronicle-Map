/*
 *      Copyright (C) 2015  higherfrequencytrading.com
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

package net.openhft.chronicle.hash.serialization.internal;

import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.hash.serialization.DeserializationFactoryConfigurableBytesReader;
import net.openhft.compiler.CompilerUtils;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.ObjectFactory;
import net.openhft.lang.model.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;

import static net.openhft.lang.model.DataValueGenerator.*;

public final class DataValueBytesMarshallers {

    private static final boolean dumpCode = Boolean.getBoolean("dvg.dumpCode");
    private static final Map<Class, Class> readersClassMap =
            new ConcurrentHashMap<>();
    private static final Map<Class, Class> readersWithCustomFactoriesClassMap =
            new ConcurrentHashMap<>();
    private static final Map<Class, Class> writerClassMap =
            new ConcurrentHashMap<>();

    public static <T> BytesReader<T> acquireBytesReader(Class<T> tClass) {
        Class readerClass = acquireReaderClass(tClass);
        try {
            Field instanceField = readerClass.getDeclaredField("INSTANCE");
            Object reader = instanceField.get(null);
            return (BytesReader<T>) reader;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    public static <T> BytesWriter<T> acquireBytesWriter(Class<T> tClass) {
        Class readerClass = acquireWriterClass(tClass);
        try {
            Field instanceField = readerClass.getDeclaredField("INSTANCE");
            Object reader = instanceField.get(null);
            return (BytesWriter<T>) reader;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }


    public static <T> Class acquireReaderClass(Class<T> tClass) {
        return readersClassMap.computeIfAbsent(tClass,
                DataValueBytesMarshallers::compileReaderClass);
    }

    private static <T> Class compileReaderClass(Class<T> tClass) {
        DataValueClasses.directClassFor(tClass);
        Class readerClass;
        DataValueModel<T> dvmodel = DataValueModels.acquireModel(tClass);
        for (Class clazz : dvmodel.nestedModels()) {
            // touch them to make sure they are loaded.
            Class clazz2 = acquireReaderClass(clazz);
        }
        String actual = generateBytesReader(tClass);
        if (dumpCode)
            LoggerFactory.getLogger(DataValueGenerator.class).info(actual);
        ClassLoader classLoader = tClass.getClassLoader();
        String className = bytesReaderName(tClass, false);
        try {
            readerClass = classLoader.loadClass(className);
        } catch (ClassNotFoundException ignored) {
            try {
                readerClass = CompilerUtils.CACHED_COMPILER
                        .loadFromJava(classLoader, className, actual);
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e);
            }
        }
        acquireReaderWithCustomFactory(tClass);
        return readerClass;
    }

    public static <T> Class acquireWriterClass(Class<T> tClass) {
        return writerClassMap.computeIfAbsent(tClass,
                DataValueBytesMarshallers::compileWriterClass);
    }

    private static <T> Class compileWriterClass(Class<T> tClass) {
        Class writerClass;
        DataValueModel<T> dvmodel = DataValueModels.acquireModel(tClass);
        for (Class clazz : dvmodel.nestedModels()) {
            // touch them to make sure they are loaded.
            Class clazz2 = acquireWriterClass(clazz);
        }
        String actual = generateBytesWriter(tClass);
        if (dumpCode)
            LoggerFactory.getLogger(DataValueGenerator.class).info(actual);
        ClassLoader classLoader = tClass.getClassLoader();
        String className = bytesWriterName(tClass, false);
        try {
            writerClass = classLoader.loadClass(className);
        } catch (ClassNotFoundException ignored) {
            try {
                writerClass = CompilerUtils.CACHED_COMPILER
                        .loadFromJava(classLoader, className, actual);
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e);
            }
        }
        return writerClass;
    }

    public static <T> Class acquireReaderWithCustomFactory(Class<T> tClass) {
        return readersWithCustomFactoriesClassMap.computeIfAbsent(tClass,
                DataValueBytesMarshallers::compileReaderWithCustomFactory);
    }

    private static <T> Class compileReaderWithCustomFactory(Class<T> tClass) {
        acquireReaderClass(tClass);
        Class c;
        String actual = generateWithCustomFactoryClass(tClass);
        if (dumpCode)
            LoggerFactory.getLogger(DataValueGenerator.class).info(actual);
        ClassLoader classLoader = tClass.getClassLoader();
        String className = withCustomFactoryName(tClass);
        try {
            c = classLoader.loadClass(className);
        } catch (ClassNotFoundException ignored) {
            try {
                c = CompilerUtils.CACHED_COMPILER
                        .loadFromJava(classLoader, className, actual);
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e);
            }
        }
        return c;
    }

    private static <T> String withCustomFactoryName(Class<T> tClass) {
        return bytesReaderName(tClass, false) + "$WithCustomFactory";
    }

    private static String generateWithCustomFactoryClass(Class tClass) {
        DataValueModel<?> dvModel = DataValueModels.acquireModel(tClass);

        SortedSet<Class> imported = newImported();
        imported.add(ObjectFactory.class);
        imported.add(NotNull.class);
        imported.add(dvModel.type());

        String simpleName = tClass.getSimpleName();

        String bytesReaderName = bytesReaderName(simpleName(tClass));
        String name = bytesReaderName + "$WithCustomFactory";
        String withCustomFactoryClass =
                "public final class " + name + " extends " + bytesReaderName + " {\n" +
                        "    private static final long serialVersionUID = 0L;\n" +
                        "\n" +
                        "    @NotNull\n" +
                        "    private final ObjectFactory<" + simpleName + "> factory;\n" +
                        "\n" +
                        "    " + name + "(@NotNull ObjectFactory<" +
                        simpleName + "> factory) {\n" +
                        "        this.factory = factory;\n" +
                        "    }\n" +
                        "\n" +
                        "    @Override\n" +
                        "    " + simpleName + " getInstance() throws Exception {\n" +
                        "        return factory.create();\n" +
                        "    }\n" +
                        "}\n";

        StringBuilder sb = new StringBuilder();
        appendPackage(dvModel, sb);
        appendImported(imported, sb);
        sb.append(withCustomFactoryClass);
        return sb.toString();
    }

    private static String bytesReaderName(Class type, boolean simple) {
        return (simple ? type.getSimpleName() : type.getName()) + "$$BytesReader";
    }

    private static String bytesReaderName(String className) {
        return className + "$$BytesReader";
    }

    public static String bytesWriterName(Class type, boolean simple) {
        return (simple ? simpleName(type) : type.getName()) + "$$BytesWriter";
    }

    private static String generateBytesReader(Class<?> tClass) {
        DataValueModel<?> dvModel = DataValueModels.acquireModel(tClass);

        SortedSet<Class> imported = newImported();
        imported.add(BytesReader.class);
        imported.add(DeserializationFactoryConfigurableBytesReader.class);
        imported.add(ObjectFactory.class);
        imported.add(Bytes.class);
        imported.add(ByteableMarshaller.class);
        imported.add(Byteable.class);
        imported.add(dvModel.type());
        imported.add(InvocationTargetException.class);
        imported.add(Constructor.class);

        StringBuilder read = generateReadBody(dvModel, imported);

        StringBuilder sb = new StringBuilder();
        appendPackage(dvModel, sb);
        appendImported(imported, sb);
        String simpleName = tClass.getSimpleName();
        String bytesReaderName = bytesReaderName(tClass, false);
        String simpleReaderName = bytesReaderName(simpleName(tClass));
        sb.append("\n@SuppressWarnings(\"unchecked\") public class ").append(simpleReaderName)
                .append(" implements DeserializationFactoryConfigurableBytesReader<")
                .append(simpleName).append(", ").append(simpleReaderName).append("> {\n");
        declareSerialVersionUID(sb);
        declareStaticInstance(sb, tClass);
        generatePrivateConstructor(sb, simpleReaderName);
        generateWithDeserializationFactory(tClass, dvModel, sb, simpleName, simpleReaderName);
        generateGetInstance(sb, tClass);
        generateDelegatingRead(sb, simpleName);
        generateRead(read, sb, simpleName, tClass);
        sb.append("}\n");
        return sb.toString();
    }

    private static void generateWithDeserializationFactory(Class<?> tClass,
                                                           DataValueModel<?> dvModel,
                                                           StringBuilder sb, String simpleName,
                                                           String simpleReaderName) {
        appendOverride(sb);
        sb.append("    public ").append(simpleReaderName)
                .append(" withDeserializationFactory(ObjectFactory<").append(simpleName)
                .append("> factory) {\n");
        sb.append("        try {\n");
        sb.append("            ")
                .append("Class cfc = Class.forName(\"")
                .append(getPackage(dvModel))
                .append(".").append(withCustomFactoryName(tClass)).append("\");\n");
        sb.append(
                "            Constructor constructor = cfc.getConstructor(ObjectFactory.class);\n" +
                        "            Object reader = constructor.newInstance(factory);\n" +
                        "            return (" + simpleReaderName + ") reader;\n" +
                        "        } catch (ClassNotFoundException | IllegalAccessException | " +
                        "NoSuchMethodException | InstantiationException |\n" +
                        "                InvocationTargetException e) {\n" +
                        "            throw new AssertionError(e);\n" +
                        "        }\n");
        sb.append("    }\n\n");
    }

    private static StringBuilder generateReadBody(DataValueModel<?> dvModel,
                                                  SortedSet<Class> imported) {
        StringBuilder read = new StringBuilder();

        Map.Entry<String, FieldModel>[] entries =
                heapSizeOrderedFieldsGrouped(dvModel);
        for (Map.Entry<String, FieldModel> entry : entries) {
            String name = entry.getKey();
            FieldModel model = entry.getValue();
            Class type = model.type();
            if (shouldImport(type))
                imported.add(type);

            Method setter = getSetter(model);
            Method getter = getGetter(model);

            Method orderedSetter = getOrderedSetter(model);
            Method volatileGetter = getVolatileGetter(model);

            Method defaultSetter = setter != null ? setter : orderedSetter;
            Method defaultGetter = getter != null ? getter : volatileGetter;
            if (!dvModel.isScalar(type)) {
                if (model.isArray()) {
                    read.append("        for (int i = 0; i < ")
                            .append(model.indexSize().value()).append("; i++) {\n");
                    read.append("            toReuse.")
                            .append(defaultSetter.getName()).append("(i, ")
                            .append(bytesReaderName(type, false))
                            .append(".INSTANCE").append(".read(bytes, ")
                            .append(computeNonScalarOffset(dvModel, type)).append(", ")
                            .append("toReuse.").append(defaultGetter.getName()).append("(i)));\n");
                    read.append("        }\n");
                } else {
                    read.append("        toReuse.")
                            .append(defaultSetter.getName()).append("(")
                            .append(bytesReaderName(type, false))
                            .append(".INSTANCE").append(".read(bytes, ")
                            .append(computeNonScalarOffset(dvModel, type)).append(", ")
                            .append("toReuse.").append(defaultGetter.getName()).append("()));\n");
                }
            } else {
                if (model.isArray()) {
                    read.append("        for (int i = 0; i < ")
                            .append(model.indexSize().value()).append("; i++) {\n");
                    saveCharSequencePosition(read, type);
                    read.append("            toReuse.").append(defaultSetter.getName())
                            .append("(i, bytes.read").append(bytesType(type)).append("());\n");
                    updateCharSequencePosition(read, model, type);
                    read.append("        }\n");
                } else {
                    read.append("        {\n");
                    saveCharSequencePosition(read, type);
                    read.append("            toReuse.").append(defaultSetter.getName())
                            .append("(bytes.read").append(bytesType(type)).append("());\n");
                    updateCharSequencePosition(read, model, type);
                    read.append("        }\n");
                }
            }
        }
        return read;
    }

    private static void updateCharSequencePosition(StringBuilder read, FieldModel model,
                                                   Class type) {
        if (CharSequence.class.isAssignableFrom(type))
            read.append("            bytes.position(pos + ")
                    .append(fieldSize(model)).append(");\n");
    }

    private static String generateBytesWriter(Class<?> tClass) {
        DataValueModel<?> dvModel = DataValueModels.acquireModel(tClass);

        SortedSet<Class> imported = newImported();
        imported.add(BytesWriter.class);
        imported.add(Bytes.class);
        imported.add(Byteable.class);
        imported.add(dvModel.type());

        String write = generateWriteBody(dvModel, imported);

        StringBuilder sb = new StringBuilder();
        appendPackage(dvModel, sb);
        appendImported(imported, sb);
        sb.append("public enum ").append(bytesWriterName(tClass, true))
                .append(" implements BytesWriter<").append(tClass.getSimpleName()).append("> {\n");
        sb.append("    INSTANCE;\n\n");
        generateSize(tClass, dvModel, sb);
        generateWrite(tClass, dvModel, write, sb);
        generateShouldNotBeNull(tClass, sb);
        sb.append("}\n");
        return sb.toString();
    }

    private static void generateShouldNotBeNull(Class<?> tClass, StringBuilder sb) {
        sb.append("    private static void shouldNotBeNull() {\n");
        sb.append("        throw new NullPointerException(\"Sub-members of " +
                tClass.getSimpleName() + " shouldn't be null for this writer. You should specify" +
                " custom writer (e. g. using keyMarshallers()/valueMarshallers() methods of" +
                "ChronicleMapBuilder, if you want to support null fields/array elements.\");\n");
        sb.append("    }\n");
    }

    private static void generateWrite(Class<?> tClass, DataValueModel<?> dvModel, String write,
                                      StringBuilder sb) {
        appendOverride(sb);
        sb.append("    public void write(Bytes bytes, ")
                .append(tClass.getSimpleName()).append(" e) {\n");
        int size = computeNonScalarOffset(dvModel, tClass);
        if (size > 16) {
            sb.append("        if (e instanceof Byteable) {\n");
            sb.append(
                    "            Bytes eBytes = ((Byteable) e).bytes();\n" +
                            "            if (eBytes != null) {\n" +
                            "                bytes.write(eBytes, ((Byteable) e).offset(), " +
                            size + ");\n" +
                            "            } else {\n" +
                            "                throw new NullPointerException(" +
                            "\"You are trying to write a byteable object of \" +\n" +
                            "                        e.getClass() + \", \" +\n" +
                            "                        \"which bytes are not assigned. I. e. most likely " +
                            "the object is uninitialized.\");\n" +
                            "            }\n");
            sb.append("            return;\n");
            sb.append("        }\n");
        }
        sb.append(write);
        sb.append("    }\n\n");
    }

    private static void generateSize(Class<?> tClass, DataValueModel<?> dvModel, StringBuilder sb) {
        appendOverride(sb);
        sb.append("    public long size(").append(tClass.getSimpleName()).append(" e) {\n");
        sb.append("        return ").append(computeNonScalarOffset(dvModel, tClass)).append(";\n");
        sb.append("    }\n\n");
    }

    private static String generateWriteBody(DataValueModel<?> dvModel, SortedSet<Class> imported) {
        StringBuilder write = new StringBuilder();

        Map.Entry<String, FieldModel>[] entries =
                heapSizeOrderedFieldsGrouped(dvModel);
        for (Map.Entry<String, FieldModel> entry : entries) {
            String name = entry.getKey();
            FieldModel model = entry.getValue();
            Class type = model.type();
            if (shouldImport(type))
                imported.add(type);

            Method setter = getSetter(model);
            Method getter = getGetter(model);

            Method orderedSetter = getOrderedSetter(model);
            Method volatileGetter = getVolatileGetter(model);

            Method defaultSetter = setter != null ? setter : orderedSetter;
            Method defaultGetter = getter != null ? getter : volatileGetter;
            if (!dvModel.isScalar(type)) {
                if (model.isArray()) {
                    write.append("        for (int i = 0; i < ")
                            .append(model.indexSize().value()).append("; i++) {\n");
                    write.append("            ")
                            .append(normalize(type)).append(" $ = e.")
                            .append(defaultGetter.getName()).append("(i);\n");
                    write.append("            ")
                            .append("if ($ == null) shouldNotBeNull();\n");
                    write.append("            ")
                            .append(bytesWriterName(type, false))
                            .append(".INSTANCE").append(".write(bytes, $);\n");
                    write.append("        }\n");
                } else {
                    write.append("        {")
                            .append(normalize(type)).append(" $ = e.")
                            .append(defaultGetter.getName()).append("();\n");
                    write.append("        ")
                            .append("if ($ == null) shouldNotBeNull();\n");
                    write.append("        ")
                            .append(bytesWriterName(type, false))
                            .append(".INSTANCE").append(".write(bytes, $);}\n");
                }
            } else {
                if (model.isArray()) {
                    write.append("        for (int i = 0; i < ")
                            .append(model.indexSize().value()).append("; i++) {\n");
                    saveCharSequencePosition(write, type);
                    write.append("            bytes.write").append(bytesType(type))
                            .append("(e.").append(defaultGetter.getName()).append("(i));\n");
                    zeroOutRemainingCharSequenceBytesAndUpdatePosition(write, model, type);
                    write.append("        }\n");
                } else {
                    write.append("        {\n");
                    saveCharSequencePosition(write, type);
                    write.append("            bytes.write").append(bytesType(type))
                            .append("(e.").append(defaultGetter.getName()).append("());\n");
                    zeroOutRemainingCharSequenceBytesAndUpdatePosition(write, model, type);
                    write.append("        }\n");
                }
            }
        }
        return write.toString();
    }

    private static void saveCharSequencePosition(StringBuilder write, Class type) {
        if (CharSequence.class.isAssignableFrom(type))
            write.append("            long pos = bytes.position();\n");
    }

    private static void zeroOutRemainingCharSequenceBytesAndUpdatePosition(
            StringBuilder write, FieldModel model, Class type) {
        if (CharSequence.class.isAssignableFrom(type)) {
            write.append("            long newPos = pos + ").append(fieldSize(model))
                    .append(";\n");
            write.append("            bytes.zeroOut(bytes.position(), newPos);\n");
            write.append("            bytes.position(newPos);\n");
        }
    }

    private static void generateRead(StringBuilder read, StringBuilder sb, String simpleName,
                                     Class cl) {
        appendOverride(sb);
        sb.append("    public ").append(simpleName).append(" read(Bytes bytes, long size, ")
                .append(simpleName).append(" toReuse) {\n");
        sb.append("    try {\n");
        sb.append("        if (toReuse == null)\n");
        sb.append("            toReuse = getInstance();\n");
        sb.append("        if (toReuse instanceof Byteable) {\n");
        sb.append("            " +
                "ByteableMarshaller.setBytesAndOffset(((Byteable) toReuse), bytes);\n");
        sb.append("            bytes.skip(size);\n");
        sb.append("            return toReuse;\n");
        sb.append("        }\n");
        sb.append(read);
        sb.append("        return toReuse;\n");
        sb.append("    } catch (Exception e) {\n");
        sb.append("        throw new IllegalStateException(e);\n");
        sb.append("    }\n");
        sb.append("}\n\n");
    }

    private static void generateDelegatingRead(StringBuilder sb, String simpleName) {
        appendOverride(sb);
        sb.append("    public ").append(simpleName).append(" read(Bytes bytes, long size) {\n");
        sb.append("        return read(bytes, size, null);\n");
        sb.append("    }\n\n");
    }

    private static void appendOverride(StringBuilder sb) {
        sb.append("    @Override\n");
    }

    private static void generatePrivateConstructor(StringBuilder sb, String bytesReaderName) {
        sb.append("    ").append(bytesReaderName).append("() {}\n\n");
    }

    private static void declareStaticInstance(StringBuilder sb, Class cl) {
        String bytesReaderName = bytesReaderName(simpleName(cl));
        sb.append("    public static final ").append(bytesReaderName).append(" INSTANCE = new ")
                .append(bytesReaderName).append("();\n\n");
    }

    private static StringBuilder declareSerialVersionUID(StringBuilder sb) {
        return sb.append("    private static final long serialVersionUID = 0L;\n\n");
    }

    private static void generateGetInstance(StringBuilder sb, Class cl) {
        sb.append("    ").append(cl.getCanonicalName())
                .append(" getInstance() throws Exception {\n");
        sb.append("        ").append("return new ").append(cl.getName()).append("$$Native();\n");
        sb.append("    ").append("}\n\n");
    }
}
