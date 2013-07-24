package com.appmetr.hercules.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.*;
import java.util.Collection;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SerializationUtils {

    private static final int UTF_FLAG_NULL = 0;
    private static final int UTF_FLAG_OLD_STRING = 1;
    private static final int UTF_FLAG_BYTES = 2;

    public static <T extends Serializable> byte[] serialize(T obj) {
        try {
            ByteArrayOutputStream baos = null;
            try {
                baos = new ByteArrayOutputStream();

                ObjectOutput out = null;
                try {
                    out = new ObjectOutputStream(baos);
                    out.writeObject(obj);
                    out.flush();
                } finally { if (out != null) { try { out.close(); } catch (Exception e) { /* NOP */ } } }

                baos.flush();
                return baos.toByteArray();
            } finally { if (baos != null) { try { baos.close(); } catch (Exception e) { /* NOP */ } } }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Serializable> T deserialize(byte[] buf) {
        try {
            ByteArrayInputStream bis = null;
            try {
                bis = new ByteArrayInputStream(buf);

                ObjectInputStream oin = null;
                try {
                    oin = new ObjectInputStream(bis);
                    return (T) oin.readObject();
                } finally { if (oin != null) { try { oin.close(); } catch (IOException e) { /* NOP */ } } }

            } finally { if (bis != null) { try { bis.close(); } catch (IOException e) { /* NOP */ } } }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Externalizable> byte[] serialize(T obj) {
        try {
            return serializeSafe(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Externalizable> void deserialize(byte[] buf, T obj) {
        try {
            deserializeSafe(buf, obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Externalizable> byte[] serializeGZip(T obj) {
        try {
            return serializeGZipSafe(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Externalizable> void deserializeGZip(byte[] buf, T obj) {
        try {
            deserializeGZipSafe(buf, obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Externalizable> byte[] serializeSafe(T obj) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();

            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(baos);
                obj.writeExternal(out);
                out.flush();
            } finally { if (out != null) { try { out.close(); } catch (Exception e) { /* NOP */ } } }

            baos.flush();
            return baos.toByteArray();
        } finally { if (baos != null) { try { baos.close(); } catch (Exception e) { /* NOP */ } } }
    }

    public static <T extends Externalizable> void deserializeSafe(byte[] buf, T obj) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = null;
        try {
            bis = new ByteArrayInputStream(buf);

            ObjectInputStream oin = null;
            try {
                oin = new ObjectInputStream(bis);
                obj.readExternal(oin);
            } finally { if (oin != null) { try { oin.close(); } catch (IOException e) { /* NOP */ } } }

        } finally { if (bis != null) { try { bis.close(); } catch (IOException e) { /* NOP */ } } }
    }

    public static <T extends Externalizable> byte[] serializeGZipSafe(T obj) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();

            GZIPOutputStream gzip = null;
            try {
                gzip = new GZIPOutputStream(baos);

                ObjectOutput out = null;
                try {
                    out = new ObjectOutputStream(gzip);
                    obj.writeExternal(out);
                    out.flush();
                } finally { if (out != null) { try { out.close(); } catch (Exception e) { /* NOP */ } } }

                gzip.finish();
                gzip.flush();
            } finally { if (gzip != null) { try { gzip.close(); } catch (Exception e) { /* NOP */ } } }

            baos.flush();
            return baos.toByteArray();
        } finally { if (baos != null) { try { baos.close(); } catch (Exception e) { /* NOP */ } } }
    }

    public static <T extends Externalizable> void deserializeGZipSafe(byte[] buf, T obj) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = null;
        try {
            bis = new ByteArrayInputStream(buf);

            GZIPInputStream gzip = null;
            try {
                gzip = new GZIPInputStream(bis);

                ObjectInputStream oin = null;
                try {
                    oin = new ObjectInputStream(gzip);
                    obj.readExternal(oin);
                } finally { if (oin != null) { try { oin.close(); } catch (IOException e) { /* NOP */ } } }

            } finally { if (gzip != null) { try { gzip.close(); } catch (IOException e) { /* NOP */ } } }

        } finally { if (bis != null) { try { bis.close(); } catch (IOException e) { /* NOP */ } } }
    }

    public static void writeNullUTF(DataOutput out, String s) throws IOException {
        if (s == null) {
            out.writeInt(UTF_FLAG_NULL);
            return;
        }
        byte[] buf = s.getBytes("UTF-8");
        writeNullBytes(out, buf);
    }

    public static void writeNullBytes(DataOutput out, byte[] buf) throws IOException {
        if (buf == null) {
            out.writeInt(UTF_FLAG_NULL);
        } else {
            out.writeInt(UTF_FLAG_BYTES);

            out.writeInt(buf.length);
            out.write(buf);
        }
    }

    public static int calcNullUTFSerializedSize(String s) {
        try {
            if (s == null) {

                return 4;
            } else {
                return 4 + s.getBytes("UTF-8").length + 4;
            }
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static byte[] readNullBytes(DataInput in) throws IOException {
        int flag = in.readInt();
        if (flag == UTF_FLAG_NULL) {

            return null;
        } else if (flag == UTF_FLAG_BYTES) {

            int bufLength = in.readInt();
            byte[] buf = new byte[bufLength];
            in.readFully(buf);
            return buf;
        } else {

            throw new IOException("Read UTF - invalid flag: " + flag);
        }
    }

    public static String readNullUTF(DataInput in) throws IOException {
        int flag = in.readInt();
        if (flag == UTF_FLAG_NULL) {

            return null;
        } else if (flag == UTF_FLAG_OLD_STRING) {

            return in.readUTF();
        } else if (flag == UTF_FLAG_BYTES) {

            int bufLength = in.readInt();
            byte[] buf = new byte[bufLength];
            in.readFully(buf);
            return new String(buf, "UTF-8");
        } else {

            throw new IOException("Read UTF - invalid flag: " + flag);
        }
    }

    public static void writeCollectionString(DataOutput out, Collection<String> list) throws IOException {
        out.writeInt(list.size());
        for (String item : list) {
            SerializationUtils.writeNullUTF(out, item);
        }
    }

    public static void readCollectionString(DataInput in, Collection<String> buffer) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            buffer.add(SerializationUtils.readNullUTF(in));
        }
    }

    //Null safe java object serialization
    public static void writeNullSafeBoolean(DataOutput out, Boolean value) throws IOException {
        boolean isNull = value == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeBoolean(value);
        }
    }

    public static Boolean readNullSafeBoolean(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();

        if (!isNull) {
            return in.readBoolean();
        }

        return null;
    }

    public static void writeNullSafeShort(DataOutput out, Short value) throws IOException {
        boolean isNull = value == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeShort(value);
        }
    }

    public static Short readNullSafeShort(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();

        if (!isNull) {
            return in.readShort();
        }

        return null;
    }

    public static void writeNullSafeInt(DataOutput out, Integer value) throws IOException {
        boolean isNull = value == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeInt(value);
        }
    }

    public static Integer readNullSafeInt(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();

        if (!isNull) {
            return in.readInt();
        }

        return null;
    }

    public static void writeNullSafeLong(DataOutput out, Long value) throws IOException {
        boolean isNull = value == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeLong(value);
        }
    }

    public static Long readNullSafeLong(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();

        if (!isNull) {
            return in.readLong();
        }

        return null;
    }

    public static void writeNullSafeDateTime(DataOutput out, DateTime date) throws IOException {
        boolean isNull = date == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeLong(date.getMillis());
            writeNullUTF(out, date.getZone().getID());
        }
    }

    public static DateTime readNullSafeDateTime(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();

        if (!isNull) {
            long millis = in.readLong();
            String tzId = readNullUTF(in);
            return new DateTime(millis, DateTimeZone.forID(tzId));
        }
        return null;
    }


    public static void writeNullSafeFloat(DataOutput out, Float value) throws IOException {
        boolean isNull = value == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeFloat(value);
        }
    }

    public static Float readNullSafeFloat(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();

        if (!isNull) {
            return in.readFloat();
        }

        return null;
    }

    public static void writeNullSafeDouble(DataOutput out, Double value) throws IOException {
        boolean isNull = value == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeDouble(value);
        }
    }

    public static Double readNullSafeDouble(DataInput in) throws IOException {
        boolean isNull = in.readBoolean();

        if (!isNull) {
            return in.readDouble();
        }

        return null;
    }

    public static boolean isGZipStream(byte[] bytes) {
        int head = ((int) bytes[0] & 0xff) | ((bytes[1] << 8) & 0xff00);
        return (GZIPInputStream.GZIP_MAGIC == head);
    }


}
