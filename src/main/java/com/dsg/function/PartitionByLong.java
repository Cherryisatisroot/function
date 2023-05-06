/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.dsg.function;


import com.dsg.rule.RuleAlgorithm;
import com.dsg.util.PartitionUtil;
import com.dsg.util.SplitUtil;


public final class PartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = -4712399083043025898L;
    protected int[] count;
    protected int[] length;
    protected PartitionUtil partitionUtil;
    private int hashCode = 1;

    /**
     * 将字符串转换为int类型数组，字符串需要以“ , ”进行分隔
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

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
        propertiesMap.put("partitionCount", partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
        propertiesMap.put("partitionLength", partitionLength);
    }

    /**
     * 根据设置的count和length，用PartitionUtil对象，生成需要用到的数组
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
     * 根据传入的参数，使用partitionUtil.partition 计算返回对应的分区值
     * @param key
     * @return
     */
    private Integer calculate(long key) {
        return partitionUtil.partition(key);
    }

    /**
     * 根据传入的字符串，计算出对应的分区值
     * @param columnValue
     * @return
     * 1.传入的参数为null 或者为 NULL时，直接返回0
     * 2.将参数解析为long类型后调用calculate方法计算分区值
     */
    @Override
    public Integer calculate(String columnValue) {
        try {
            if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                return 0;
            }
            long key = Long.parseLong(columnValue);
            return calculate(key);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
    }

    /**
     * 双参数
     * @param beginValue 开始值
     * @param endValue  结束值
     * @return
     * 1.当传入的参数解析格式不正确时，直接返回integer类型的空数组
     * 2.使用partitionUtil获取生成的数组长度，和begin，end进行相关计算确定是否返回integer类型的空数组
     * 3.begin和end调用calculate方法计算对应的分区值，付给beginNode和endNode，做相关判断来进行相关运算
     */
    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        long begin = 0;
        long end = 0;
        try {
            begin = Long.parseLong(beginValue);
            end = Long.parseLong(endValue);
        } catch (NumberFormatException e) {
            return new Integer[0];
        }
        int partitionLength = partitionUtil.getPartitionLength();
        if (end - begin >= partitionLength || begin > end) { //TODO: optimize begin > end
            return new Integer[0];
        }
        Integer beginNode = calculate(begin);
        Integer endNode = calculate(end);

        //当endNode大于beginNode或者endNode和beginNode相等 且 begin和end相等为单节点时
        if (endNode > beginNode || (endNode.equals(beginNode) && partitionUtil.isSingleNode(begin, end))) {
            int len = endNode - beginNode + 1;
            //定义长度为endNode-beginNode + 1 的integer数组
            Integer[] re = new Integer[len];
            //使用循环，给数组进行赋值
            for (int i = 0; i < len; i++) {
                re[i] = beginNode + i;
            }
            return re;
        } else {
            //segmentLength的值为count数组之和
            int split = partitionUtil.getSegmentLength() - beginNode;
            int len = split + endNode + 1;
            //endNode和beginNode相等时，长度-1，去除重复项
            if (endNode.equals(beginNode)) {
                //remove duplicate
                len--;
            }
            //以len为长度创建integer类型的数组
            Integer[] re = new Integer[len];
            for (int i = 0; i < split; i++) {
                re[i] = beginNode + i;
            }
            for (int i = split; i < len; i++) {
                re[i] = i - split;
            }
            return re;
        }
    }

    /**
     * getPartitionNum, return -1 means no limit
     *
     * @return partitionNum
     */
    @Override
    public int getPartitionNum() {
        return partitionUtil.getSegmentLength();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionByLong other = (PartitionByLong) o;
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
        return true;
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
    }

}
