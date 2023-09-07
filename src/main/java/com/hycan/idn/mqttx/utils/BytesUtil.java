package com.hycan.idn.mqttx.utils;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import javax.imageio.stream.FileImageOutputStream;

import org.apache.tomcat.util.codec.binary.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BytesUtil {
    private static final Logger log = LoggerFactory.getLogger(BytesUtil.class);
    public static final DateTimeFormatter dateTimeformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter shortTimeFormatter = DateTimeFormatter.ofPattern("yyMMddHHmmss");

    public BytesUtil() {
    }

    public static byte[] string2ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }

    public static String ascii2string(byte[] value) {
        return new String(value, StandardCharsets.US_ASCII);
    }

    public static String bytes2bit(byte[] data) {
        StringBuffer bitData = new StringBuffer();
        byte[] var2 = data;
        int var3 = data.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            byte byteData = var2[var4];
            bitData.append(getBinaryStrFromByte(byteData));
        }

        return bitData.toString();
    }


    public static byte[] bytes2full(byte[] data, Integer length) {
        byte[] resBytes = new byte[length];
        if (data.length > length) {
            System.arraycopy(data, 0, resBytes, 0, length);
        } else {
            System.arraycopy(data, 0, resBytes, 0, data.length);
        }

        return resBytes;
    }

    public static String byte2bit(byte b) {
        return getBinaryStrFromByte(b);
    }


    public static byte parseIntToByte(int num) {
        return (byte)num;
    }

    public static int byte2int(byte b) {
        return b & 255;
    }

    public static byte[] longToByteArray(long value) {
        byte[] b = new byte[8];

        for(int i = 0; i < 8; ++i) {
            int offset = (b.length - 1 - i) * 8;
            b[i] = (byte)((int)(value >>> offset & 255L));
        }

        return b;
    }

    public static byte[] ObjectToByte(Object obj) {
        byte[] bytes = null;

        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bo);
            oo.writeObject(obj);
            bytes = bo.toByteArray();
            bo.close();
            oo.close();
        } catch (Exception var4) {
            log.warn("translation" + var4.getMessage());
            var4.printStackTrace();
        }

        return bytes;
    }


    public static int parseBytesToInt(byte[] bytes) {
        int length = bytes.length;
        int intValue = 0;

        for(int i = 0; i < length; ++i) {
            intValue <<= 8;
            intValue |= bytes[i] & 255;
        }

        return intValue;
    }

    public static float parseBytesToFloat(byte[] bytes, int offset, int accuracy) {
        int length = bytes.length;
        int intValue = 0;

        for(int i = 0; i < length; ++i) {
            intValue <<= 8;
            intValue |= bytes[i] & 255;
        }

        float floatValue;
        if (intValue == 65534) {
            floatValue = 999999.0F;
        } else if (intValue == 65535) {
            floatValue = -999999.0F;
        } else {
            floatValue = (float)(intValue - offset) / (float)accuracy;
        }

        return floatValue;
    }

    public static double parseBytesToDouble(byte[] bytes, int offset, int accuracy) {
        int length = bytes.length;
        int intValue = 0;

        for(int i = 0; i < length; ++i) {
            intValue <<= 8;
            intValue |= bytes[i] & 255;
        }

        double doubleValue;
        if (intValue == -2) {
            doubleValue = 9.9999999E7;
        } else if (intValue == -1) {
            doubleValue = -9.9999999E7;
        } else {
            doubleValue = (double)(intValue - offset) / (double)accuracy;
        }

        return doubleValue;
    }

    public static float parseByteToFloat(byte value, int offset, int accuracy) {
        if (value == 254) {
            return 999999.0F;
        } else if (value == 255) {
            return -999999.0F;
        } else {
            int tmp = value & 255;
            return (float)((tmp - offset) / accuracy);
        }
    }

    public static int checkBytesToInt(byte[] bytes) {
        int length = bytes.length;
        int intValue = 0;

        for(int i = 0; i < length; ++i) {
            intValue <<= 8;
            intValue |= bytes[i] & 255;
        }

        if (intValue == 65534) {
            intValue = 999999;
        } else if (intValue == 65535) {
            intValue = -999999;
        }

        return intValue;
    }

    public static String binBytesToInt(String msgBytes) {
        String tempInt = Integer.valueOf(msgBytes, 2).toString();
        return tempInt;
    }


    public static short parseBytesToShort(byte[] bytes) {
        int length = bytes.length;
        short intValue = 0;

        for(int i = 0; i < length; ++i) {
            intValue = (short)(intValue << 8);
            intValue = (short)(intValue | bytes[i] & 255);
        }

        return intValue;
    }

    public static String bcdToStr(byte[] bytes) {
        StringBuffer temp = new StringBuffer(bytes.length * 2);

        for(int i = 0; i < bytes.length; ++i) {
            temp.append((byte)((bytes[i] & 240) >>> 4));
            temp.append((byte)(bytes[i] & 15));
        }

        return temp.toString().substring(0, 1).equalsIgnoreCase("0") ? temp.toString().substring(1) : temp.toString();
    }

    public static LocalDateTime byteToTime(byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < bytes.length; ++i) {
            if (bytes[i] < 10) {
                sb.append("0");
            }

            sb.append(bytes[i]);
        }

        String time = sb.toString();
        LocalDateTime date = LocalDateTime.parse(time, shortTimeFormatter);
        date.atOffset(ZoneOffset.of("+8"));
        return date;
    }

    public static byte[] timeToBytes(LocalDateTime date) throws ParseException {
        String dateStr = shortTimeFormatter.format(date);
        byte[] time = new byte[6];

        for(int i = 0; i < 6; ++i) {
            int value = Integer.valueOf(dateStr.substring(2 * i, 2 * i + 2));
            time[i] = (byte)value;
        }

        return time;
    }

    public static String BcdToStr(byte[] bytes) {
        StringBuffer temp = new StringBuffer(bytes.length * 2);

        for(int i = 0; i < bytes.length; ++i) {
            temp.append((byte)((bytes[i] & 240) >>> 4));
            temp.append((byte)(bytes[i] & 15));
        }

        return temp.toString();
    }

    public static String bcdToStr(byte b) {
        StringBuffer temp = new StringBuffer(2);
        temp.append((byte)((b & 240) >>> 4));
        temp.append((byte)(b & 15));
        return temp.toString().substring(0, 1).equalsIgnoreCase("0") ? temp.toString().substring(1) : temp.toString();
    }

    public static String BcdToStr(byte b) {
        StringBuffer temp = new StringBuffer(2);
        temp.append((byte)((b & 240) >>> 4));
        temp.append((byte)(b & 15));
        return temp.toString();
    }

    public static long parseBytesToLong(byte[] bytes) {
        long longValue = 0L;

        for(int i = 0; i < bytes.length; ++i) {
            longValue <<= 8;
            longValue |= (long)(bytes[i] & 255);
        }

        return longValue;
    }

    public static byte[] cutBytes(int byteIndex, int byteLength, byte[] bytes) {
        byte[] result = new byte[byteLength];
        System.arraycopy(bytes, byteIndex, result, 0, byteLength);
        return result;
    }

    public static int getBitValue(int byteIndex, int bitIndex, byte[] bytes) {
        byte byteValue = bytes[byteIndex];
        return byteValue >> 7 - bitIndex & 1;
    }

    public static boolean getBooleanValue(int byteIndex, int bitIndex, byte[] bytes) {
        byte byteValue = bytes[byteIndex];
        return (byteValue >> 7 - bitIndex & 1) == 1;
    }

    public static boolean getBooleanValues(int bitIndex, byte bytes) {
        String bytesToString = getBinaryStrFromByte(bytes);
        String bit = bytesToString.substring(bitIndex, bitIndex + 1);
        int bits = Integer.parseInt(bit);
        return bits == 1;
    }

    public static String getBinaryStrFromByte(byte b) {
        String result = "";
        byte a = b;

        for(int i = 0; i < 8; ++i) {
            byte c = a;
            a = (byte)(a >> 1);
            a = (byte)(a << 1);
            if (a == c) {
                result = "0" + result;
            } else {
                result = "1" + result;
            }

            a = (byte)(a >> 1);
        }

        return result;
    }

    public static String hexString2binaryString(String hexString) {
        if (hexString != null && hexString.length() % 2 == 0) {
            String bString = "";

            for(int i = 0; i < hexString.length(); ++i) {
                String tmp = "0000" + Integer.toBinaryString(Integer.parseInt(hexString.substring(i, i + 1), 16));
                bString = bString + tmp.substring(tmp.length() - 4);
            }

            return bString;
        } else {
            return null;
        }
    }

    public static byte getByte(int byteIndex, byte[] bytes) {
        return bytes[byteIndex];
    }

    public static int getIntFromBytes(int byteIndex, byte[] bytes) {
        byte value = bytes[byteIndex];
        return Byte.toUnsignedInt(value);
    }

    public static byte[] getBigWord(int byteIndex, byte[] bytes) {
        byte[] result = new byte[]{bytes[byteIndex + 1], bytes[byteIndex]};
        return result;
    }

    public static byte[] getBigDWord(int byteIndex, byte[] bytes) {
        byte[] result = new byte[]{bytes[byteIndex + 3], bytes[byteIndex + 2], bytes[byteIndex + 1], bytes[byteIndex]};
        return result;
    }

    public static byte[] getWord(int byteIndex, byte[] bytes) {
        return cutBytes(byteIndex, 2, bytes);
    }

    public static byte[] getDWord(int byteIndex, byte[] bytes) {
        return cutBytes(byteIndex, 4, bytes);
    }

    public static int getBitsValue(int bitIndex, int bitLength, byte[] bytes) {
        int intValue = parseBytesToInt(bytes);
        int clearValue = 0;

        for(int i = 0; i < bitLength; ++i) {
            clearValue <<= 1;
            ++clearValue;
        }

        return intValue >> bytes.length * 8 - (bitIndex + bitLength - 1) & clearValue;
    }

    public static String bytesToHexString(byte[] src) {
        StringBuilder stringBuilder = new StringBuilder("");
        if (src != null && src.length > 0) {
            for(int i = 0; i < src.length; ++i) {
                int v = src[i] & 255;
                String hv = Integer.toHexString(v);
                if (hv.length() < 2) {
                    stringBuilder.append(0);
                }

                stringBuilder.append(hv);
            }

            return stringBuilder.toString();
        } else {
            return null;
        }
    }

    public static byte[] toStringHex(String hexString) {
        byte[] baKeyword = new byte[hexString.length() / 2];

        for(int i = 0; i < baKeyword.length; ++i) {
            try {
                baKeyword[i] = (byte)(255 & Integer.parseInt(hexString.substring(i * 2, i * 2 + 2), 16));
            } catch (Exception var4) {
                log.warn("", var4);
            }
        }

        return baKeyword;
    }

    public static byte[] strToBcd(String asc) {
        int len = asc.length();
        int mod = len % 2;
        if (mod != 0) {
            asc = "0" + asc;
            len = asc.length();
        }

        byte[] abt = new byte[len];
        if (len >= 2) {
            len /= 2;
        }

        byte[] bbt = new byte[len];
        abt = asc.getBytes();

        for(int p = 0; p < asc.length() / 2; ++p) {
            int j;
            if (abt[2 * p] >= 48 && abt[2 * p] <= 57) {
                j = abt[2 * p] - 48;
            } else if (abt[2 * p] >= 97 && abt[2 * p] <= 122) {
                j = abt[2 * p] - 97 + 10;
            } else {
                j = abt[2 * p] - 65 + 10;
            }

            int k;
            if (abt[2 * p + 1] >= 48 && abt[2 * p + 1] <= 57) {
                k = abt[2 * p + 1] - 48;
            } else if (abt[2 * p + 1] >= 97 && abt[2 * p + 1] <= 122) {
                k = abt[2 * p + 1] - 97 + 10;
            } else {
                k = abt[2 * p + 1] - 65 + 10;
            }

            int a = (j << 4) + k;
            byte b = (byte)a;
            bbt[p] = b;
        }

        return bbt;
    }

    public static byte[] int2bytes2(int value) {
        byte[] ret = new byte[]{0, (byte)(value & 255)};
        value >>= 8;
        ret[0] = (byte)(value & 255);
        return ret;
    }


    public static byte[] int2byteArray4(int value) {
        byte[] result = new byte[]{(byte)(value >> 24 & 255), (byte)(value >> 16 & 255), (byte)(value >> 8 & 255), (byte)(value & 255)};
        return result;
    }

    public static byte[] int2BigEndianBytes4(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(value);
        return buffer.array();
    }

    public static byte[] intToByteArray(int value) {
        byte[] b = new byte[4];

        for(int i = 0; i < 4; ++i) {
            int offset = (b.length - 1 - i) * 8;
            b[i] = (byte)(value >>> offset & 255);
        }

        return b;
    }

    public static LocalDateTime str2Date(String dateStr) {
        LocalDateTime dateTime = LocalDateTime.parse(dateStr, shortTimeFormatter);
        return dateTime;
    }

    public static String stringToAscii(String value) {
        StringBuffer sbu = new StringBuffer();
        char[] chars = value.toCharArray();

        for(int i = 0; i < chars.length; ++i) {
            if (i != chars.length - 1) {
                sbu.append(chars[i]).append(",");
            } else {
                sbu.append(chars[i]);
            }
        }

        return sbu.toString();
    }

    public static String hextoAscii(String s) {
        byte[] baKeyword = new byte[s.length() / 2];

        for(int i = 0; i < baKeyword.length; ++i) {
            try {
                baKeyword[i] = (byte)(255 & Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16));
            } catch (Exception var5) {
                var5.printStackTrace();
            }
        }

        try {
            s = new String(baKeyword, "ASCII");
        } catch (Exception var4) {
            var4.printStackTrace();
        }

        return s;
    }

    public static String StringToAsciiString(String content) {
        String result = "";
        int max = content.length();

        for(int i = 0; i < max; ++i) {
            char c = content.charAt(i);
            String b = Integer.toHexString(c);
            result = result + b;
        }

        return result;
    }

    public static double formatMileageData(double mileage) {
        DecimalFormat df = new DecimalFormat("###.00");
        return Double.valueOf(df.format(mileage));
    }

    public static final byte[] Integer4HexBytes(int src, int bit) {
        byte[] yaIntDatas = new byte[4];

        for(int i = bit - 1; i >= 0; --i) {
            yaIntDatas[bit - 1 - i] = (byte)(src >> 8 * i & 255);
        }

        return yaIntDatas;
    }

    public static final byte[] Integer2HexBytes(int src, int bit) {
        byte[] yaIntDatas = new byte[2];

        for(int i = bit - 1; i >= 0; --i) {
            yaIntDatas[bit - 1 - i] = (byte)(src >> 8 * i & 255);
        }

        return yaIntDatas;
    }

    public static long strToDateLong(String strDate) {
        LocalDateTime strToDate = LocalDateTime.parse(strDate, dateTimeformatter);
        return strToDate.toEpochSecond(ZoneOffset.of("+8"));
    }

    public static String toDateFormat(LocalDateTime date) {
        String sTime = dateTimeformatter.format(date);
        return sTime;
    }

    public static byte[] intToHex(int value) {
        byte[] resultbyte = new byte[2];

        String resulta;
        for(resulta = Integer.toHexString(value); resulta.length() < 4; resulta = "0" + resulta) {
        }

        resultbyte[0] = Byte.parseByte(resulta.substring(0, 2));
        resultbyte[1] = Byte.parseByte(resulta.substring(2));
        return resultbyte;
    }

    public static String getHexString(int length) {
        String resulStr;
        for(resulStr = Integer.toHexString(length); resulStr.length() < 4; resulStr = "0" + resulStr) {
        }

        return resulStr;
    }

    public static String getIntToHex(int values) {
        String msg = Integer.toHexString(values);
        if (msg.length() == 1) {
            msg = "000" + msg;
        } else if (msg.length() == 2) {
            msg = "00" + msg;
        } else if (msg.length() == 3) {
            msg = "0" + msg;
        } else {
            msg = "" + msg;
        }

        return msg;
    }

    public static String getIntToBanary(int values) {
        String msg = Integer.toBinaryString(values);
        if (msg.length() == 1) {
            msg = "000" + msg;
        } else if (msg.length() == 2) {
            msg = "00" + msg;
        } else if (msg.length() == 3) {
            msg = "0" + msg;
        } else {
            msg = "" + msg;
        }

        return msg;
    }

    public static String intTo32Binary(int n) {
        String str = "";

        for(int i = Integer.MIN_VALUE; i != 0; i >>>= 1) {
            str = str + ((n & i) == 0 ? '0' : '1');
        }

        return str;
    }

    public static String stringReverse(String msg) {
        StringBuilder sb = new StringBuilder(msg);
        sb.reverse();
        return sb.toString();
    }

    public static void byte2image(byte[] data, String path) {
        if (data.length >= 3 && !path.equals("")) {
            try {
                FileImageOutputStream imageOutput = new FileImageOutputStream(new File(path));
                imageOutput.write(data, 0, data.length);
                imageOutput.close();
                System.out.println("Make Picture success,Please find image in " + path);
            } catch (Exception var3) {
                log.warn("Exception: " + var3);
                var3.printStackTrace();
            }

        }
    }

    public static LocalDateTime strToDate(String str) {
        LocalDateTime dateTime = LocalDateTime.parse(str, dateTimeformatter);
        return dateTime;
    }

    public static String byte2HexStringNO(byte[] bytes) {
        StringBuffer temp = new StringBuffer(bytes.length * 2);

        for(int i = 0; i < bytes.length; ++i) {
            if (bytes[i] != 0) {
                temp.append((byte)((bytes[i] & 240) >>> 4));
                temp.append((byte)(bytes[i] & 15));
            }
        }

        return temp.toString().substring(0, 1).equalsIgnoreCase("0") ? temp.toString().substring(1) : temp.toString();
    }

    public static String getStringByAscByte(byte[] bytes) {
        String HexStr = byte2HexStringNO(bytes);
        String ascStr = hextoAscii(HexStr);
        return ascStr;
    }

    public static String supplementString(String str, int number, String sup) {
        int length = str.length();
        if (length >= number) {
            return str;
        } else {
            for(int i = 0; i < number - length; ++i) {
                str = sup + str;
            }

            return str;
        }
    }

    public static byte[] supplementByte(byte[] data, int number, byte sup) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int length = data.length;
        if (length < number) {
            try {
                for(int i = 0; i < number - length; ++i) {
                    bos.write(sup);
                }

                bos.write(data);
                data = bos.toByteArray();
            } catch (IOException var14) {
                var14.printStackTrace();
            } finally {
                if (bos != null) {
                    try {
                        bos.close();
                    } catch (IOException var13) {
                    }
                }

            }

            return data;
        } else {
            return data;
        }
    }

    public static int byteArrayToInt(byte[] bytes) {
        int value = 0;

        for(int i = 0; i < 4; ++i) {
            int shift = (3 - i) * 8;
            value += (bytes[i] & 255) << shift;
        }

        return value;
    }

    public static String getBinaryStrFromByte2(byte b) {
        String result = "";
        byte a = b;

        for(int i = 0; i < 8; ++i) {
            result = a % 2 + result;
            a = (byte)(a >> 1);
        }

        return result;
    }

    public static int scale2Decimal(String number, int scale) {
        if (2 <= scale && scale <= 32) {
            int total = 0;
            String[] ch = number.split("");
            int chLength = ch.length;

            for(int i = 0; i < chLength; ++i) {
                total = (int)((double)total + (double)Integer.valueOf(ch[i]) * Math.pow((double)scale, (double)(chLength - 1 - i)));
            }

            return total;
        } else {
            throw new IllegalArgumentException("scale is not in range");
        }
    }

    public static int getBitsValueByBytes(int bitIndex, int bitLength, byte[] bytes) {
        String hex = bytesToHexString(bytes);
        String binStr = hexString2binaryString(hex);
        String bitValue = binStr.substring(bitIndex, bitIndex + bitLength);
        int value = Integer.valueOf(bitValue, 2);
        return value;
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();
        return buffer.getLong();
    }

    public static byte[] longToByteArrayByLength(long value, int length) {
        byte[] b = new byte[length];

        for(int i = 0; i < length; ++i) {
            int offset = (b.length - 1 - i) * 8;
            b[i] = (byte)((int)(value >>> offset & 255L));
        }

        return b;
    }

    public static byte[] byteMerger(byte[]... data) {
        int length = 0;
        byte[][] var2 = data;
        int num = data.length;

        for(int var4 = 0; var4 < num; ++var4) {
            byte[] value = var2[var4];
            length += value.length;
        }

        byte[] byteData = new byte[length];
        num = 0;
        byte[][] var9 = data;
        int var10 = data.length;

        for(int var6 = 0; var6 < var10; ++var6) {
            byte[] value = var9[var6];
            System.arraycopy(value, 0, byteData, num, value.length);
            num += value.length;
        }

        return byteData;
    }
}

