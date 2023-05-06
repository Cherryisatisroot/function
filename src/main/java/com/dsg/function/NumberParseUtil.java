/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.dsg.function;

/**
 * 将配置文件设置的数据，解析成对应的数字
 * 2M ---> 20000
 * 200M ---> 2000000
 * 2M1 ---> 20001
 * 2K ---> 2000
 * 2k1 ---> 2001
 * 2k09 ---> 2009
 */
public final class NumberParseUtil {
    private NumberParseUtil() {
    }

    /**
     * remove the ' at the beginning and the end
     *
     * @param number
     * @return
     */
    public static String eliminateQuote(String number) {
        number = number.trim();
        if (number.contains("\"")) {
            if (number.charAt(0) == '\"') {
                number = number.substring(1);
                if (number.charAt(number.length() - 1) == '\"') {
                    number = number.substring(0, number.length() - 1);
                }
            }
        } else if (number.contains("\'")) {
            if (number.charAt(0) == '\'') {
                number = number.substring(1);
                if (number.charAt(number.length() - 1) == '\'') {
                    number = number.substring(0, number.length() - 1);
                }
            }
        }
        return number;
    }

    /**
     * can parse values like 200M ,200K,200M1(2000001)
     *
     * @param val
     * @return
     */
    public static long parseLong(String val) {
        val = val.toUpperCase();
        int index = val.indexOf("M");

        int plus = 10000;
        if (index < 0) {
            index = val.indexOf("K");
            plus = 1000;
        }
        if (index > 0) {
            String longVal = val.substring(0, index);

            long theVale = Long.parseLong(longVal) * plus;
            String remain = val.substring(index + 1);
            if (remain.length() > 0) {
                theVale += Integer.parseInt(remain);
            }
            return theVale;
        } else {
            return Long.parseLong(val);
        }

    }
}
