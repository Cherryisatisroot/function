/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.dsg.util;

import java.io.Serializable;

/**
 * PartitionUtil
 * 根据count和length计算对应的数组的工具类
 * @author mycat
 */
public final class PartitionUtil implements Serializable {

    // MAX_PARTITION_LENGTH: if the number is 2^n,  then optimizer by x % 2^n == x & (2^n - 1).
    private static final int MAX_PARTITION_LENGTH = 2880;
    private int partitionLength;

    // cached the value of  2^n - 1 because of x % 2^n == x & (2^n - 1).
    private long addValue;

    private int[] segment;

    private boolean canProfile = false;
    //记录count之和，作为元素种类的个数
    private int segmentLength = 0;

    /**
     * <pre>
     * @param count the size of partitions 每个分区的大小
     * @param length the consequent value of every partition 每个分区的结果值
     * Notice:count.length must equals length.length.  count数组的长度 == length数组的长度
     * and :MAX_PARTITION_LENGTH >=sum((count[i]*length[i]))
     * </pre>
     * 1.判断count和length是否符合要求，不能为null，且两个数组的长度需要相等
     * 2.循环遍历count和length数组，判断是否存在 <=0 的元素，存在抛异常
     * 3.依据count数组元素的和+1为长度，创建一个数组ai,其赋值通过循环根据count和length对ai数组进行赋值
     * 4.将ai数组最后一个元素作为数组segment的长度，创建数组segment，判断其长度是否大于设置的最大长度
     * 5.根据ai数据对segment数组进行赋值
     */
    public PartitionUtil(int[] count, int[] length) {
        if (count == null || length == null || (count.length != length.length)) {
            throw new RuntimeException("error,check your partitionCount & partitionLength definition.");
        }
        for (int iLength : length) {
            if (iLength <= 0) {
                throw new RuntimeException("error,make sure your partitionLength at least 1.");
            }
        }
        for (int aCount : count) {
            if (aCount <= 0) {
                throw new RuntimeException("error,make sure your partitionCount at least 1.");
            }
            segmentLength += aCount;
        }
        int[] ai = new int[segmentLength + 1];

        int index = 0;
        for (int i = 0; i < count.length; i++) {
            for (int j = 0; j < count[i]; j++) {
                ai[++index] = ai[index - 1] + length[i];
            }
        }
        partitionLength = ai[ai.length - 1];
        addValue = partitionLength - 1;
        segment = new int[partitionLength];
        if (partitionLength > MAX_PARTITION_LENGTH) {
            throw new RuntimeException("error,check your partitionScope definition.Sum(count[i]*length[i]) must be less than " + MAX_PARTITION_LENGTH);
        }
        if ((partitionLength & addValue) == 0) {
            canProfile = true;
        }

        for (int i = 1; i < ai.length; i++) {
            for (int j = ai[i - 1]; j < ai[i]; j++) {
                segment[j] = (i - 1);
            }
        }
    }

    /**
     * 判断是否为单节点
     * @param begin
     * @param end
     * @return
     */
    public boolean isSingleNode(long begin, long end) {
        if (begin == end)
            return true;
        int mod = (int) (begin % partitionLength);
        if (mod < 0) {
            mod += partitionLength;
        }
        return begin - mod + addValue >= end;
    }

    /**
     * 根据传入的hash值计算出数组索引，返回其在数组中的对应的值
     * @param hash
     * @return
     */
    public int partition(long hash) {
        if (canProfile) {
            return segment[(int) (hash & addValue)];
        } else {
            int mod = (int) (hash % partitionLength);
            if (mod < 0) {
                mod += partitionLength;
            }
            return segment[mod];
        }
    }

    public int partition(String key, int start, int end) {
        return partition(StringUtil.hash(key, start, end));
    }


    public int getPartitionLength() {
        return partitionLength;
    }

    public int getSegmentLength() {
        return segmentLength;
    }

    public static void main(String[] args) {
        int [] count = {5,6};
        int [] length = {3,4};
        PartitionUtil partitionUtil = new PartitionUtil(count, length);

    }
}
