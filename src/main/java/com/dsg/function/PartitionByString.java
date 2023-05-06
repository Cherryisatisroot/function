/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.dsg.function;



import com.dsg.rule.RuleAlgorithm;
import com.dsg.util.*;


/**
 * @author <a href="mailto:daasadmin@hp.com">yangwenx</a>
 */
public final class PartitionByString extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private static final long serialVersionUID = 3777423001153345948L;
    private int hashSliceStart = 0;
    /**
     * 0 means str.length(), -1 means str.length()-1
     */
    private int hashSliceEnd = 8;
    protected int[] count;
    protected int[] length;
    protected PartitionUtil partitionUtil;
    private int hashCode = 1;

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
        propertiesMap.put("partitionCount", partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
        propertiesMap.put("partitionLength", partitionLength);
    }


    public void setHashSlice(String hashSlice) {
        Pair<Integer, Integer> p = PairUtil.sequenceSlicing(hashSlice);
        hashSliceStart = p.getKey();
        hashSliceEnd = p.getValue();
        propertiesMap.put("hashSlice", hashSlice);
    }

    /**
     * 1.调用PartitionUtil创建对应的数组
     * 2.初始化
     */
    @Override
    public void init() {
        partitionUtil = new PartitionUtil(count, length);
        initHashCode();

    }

    @Override
    public void selfCheck() {
    }

    /**
     * 将以“ ， ”为间隔的string类型数组，转换成int类型数组
     * @param string
     * @return
     */
    private static int[] toIntArray(String string) {
        String[] strs = SplitUtil.split(string, ',', true);
        int[] ints = new int[strs.length];
        for (int i = 0; i < strs.length; ++i) {
            ints[i] = Integer.parseInt(strs[i]);
        }
        return ints;
    }

    /**
     * 单参数
     * @param key
     * @return
     * 1.判断参数是否为null 或者为 NULL 返回 0
     * 2.根据设置的hashSliceStart和hashSliceEnd进行三元运算给局部变量start和end进行赋值
     * 3.调用StringUtil的hash，传入start，end，key计算出对应的hash值
     * 4.根据partitionUtil的partition方法，传入算出的hash值计算出对应的分区值
     */
    @Override
    public Integer calculate(String key) {
        if (key == null || key.equalsIgnoreCase("NULL")) {
            return 0;
        }
        int start = hashSliceStart >= 0 ? hashSliceStart : key.length() + hashSliceStart;
        int end = hashSliceEnd > 0 ? hashSliceEnd : key.length() + hashSliceEnd;
        long hash = StringUtil.hash(key, start, end);
        return partitionUtil.partition(hash);
    }

    /**
     * 双参数
     * @param beginValue 开始值
     * @param endValue   结束值
     * @return integer类型的空数组
     */
    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        //all node
        return new Integer[0];
    }

    @Override
    public int getPartitionNum() {
        int nPartition = 0;
        for (int aCount : count) {
            nPartition += aCount;
        }
        return nPartition;
    }

    //重写equals和hashCode方法，用于进行对象间的比较运算，判断是否相等
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionByString other = (PartitionByString) o;
        if (other.count.length != this.count.length || other.length.length != this.length.length) {
            return false;
        }
        for (int i = 0; i < count.length; i++) {
            if (this.count[i] != other.count[i]) {
                return false;
            }
        }
        for (int i = 0; i < length.length; i++) {
            if (this.length[i] != other.length[i]) {
                return false;
            }
        }
        return hashSliceStart == other.hashSliceStart && hashSliceEnd == other.hashSliceEnd;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private void initHashCode() {
        for (int aCount : count) {
            hashCode *= aCount;
        }
        for (int aLength : length) {
            hashCode *= aLength;
        }
        if (hashSliceEnd - hashSliceStart != 0) {
            hashCode *= hashSliceEnd - hashSliceStart;
        }
    }
}
