/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.dsg.function;



import com.dsg.rule.RuleAlgorithm;
import com.dsg.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * PartitionByDate
 * 通过时间来进行分区的发方法
 * 1.设置时间格式  setDateFormat 必配项
 * 2.设置起始时间  setsBeginDate 必配项
 * 3.设置分区规则  setsPartionDay 必配项
 * 4.设置结束时间  setsEndDate 可选项
 * 5.设置默认节点  setDefaultNode 可选项
 * @author lxy
 */
public class PartitionByDate extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = 4966421543458534122L;

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByDate.class);

    private String sBeginDate;
    private String sEndDate;
    private String sPartionDay;
    private String dateFormat;

    private long beginDate;
    private long partitionTime;
    private long endDate;
    private int nCount;
    private int defaultNode = -1;
    private transient ThreadLocal<SimpleDateFormat> formatter;
    private static final long ONE_DAY = 86400000;
    private int hashCode = -1;

    /**
     * 分区计算器实例化时对其进行初始化
     */
    @Override
    public void init() {
        try {
            // 将分区时间转换为毫秒数
            partitionTime = Integer.parseInt(sPartionDay) * ONE_DAY;

            // 将开始时间转换为毫秒数
            beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();

            // 如果存在结束时间，则将结束时间转换为毫秒数，计算分区数量
            if (!StringUtil.isEmpty(sEndDate)) {
                endDate = new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                nCount = (int) ((endDate - beginDate) / partitionTime) + 1;
            }

            // 创建线程安全的SimpleDateFormat实例
            formatter = new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    return new SimpleDateFormat(dateFormat);
                }
            };
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }

        // 初始化哈希码
        initHashCode();
    }


    /**
     *检验参数是否合法
     */
    @Override
    public void selfCheck() {
        StringBuffer sb = new StringBuffer();
        //开始日期，不能为null或空字符串。如果为null或空字符串，会向检查结果字符串中添加一条错误信息
        if (sBeginDate == null || "".equals(sBeginDate)) {
            sb.append("sBeginDate can not be null\n");
        } else {
            try {
                new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();
            } catch (Exception e) {
                sb.append("pause beginDate error\n");
            }
        }
        //日期格式化字符串，不能为null或空字符串。如果为null或空字符串，会向检查结果字符串中添加一条错误信息
        if (dateFormat == null || "".equals(dateFormat)) {
            sb.append("dateFormat can not be null\n");
        } else {
            //结束日期，可以为null或空字符串。如果不为null或空字符串，会对它进行日期格式化，如果格式化失败，会向检查结果字符串中添加一条错误信息
            if (!StringUtil.isEmpty(sEndDate)) {
                try {
                    new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
                } catch (Exception e) {
                    sb.append("pause endDate error\n");
                }
            }
        }

        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
            throw new RuntimeException(sb.toString());
        }
    }

    /**
     *
     * @param columnValue
     * @return (int)targetPartition
     */
    @Override
    public Integer calculate(String columnValue) {
        try {
            // 如果字段为空或者为字符串"null"
            if (columnValue == null || "null".equalsIgnoreCase(columnValue)) {
                // 如果设置了默认节点
                if (defaultNode >= 0) {
                    // 返回默认节点
                    return defaultNode;
                }
                // 否则返回null
                return null;
            }
            // 将时间字符串转换为时间戳
            long targetTime = formatter.get().parse(columnValue).getTime();
            // 如果时间早于开始时间
            if (targetTime < beginDate) {
                // 如果设置了默认节点
                return (defaultNode >= 0) ? defaultNode : null;
            }
            // 计算当前时间所属的分区
            int targetPartition = (int) ((targetTime - beginDate) / partitionTime);

            // 如果时间晚于结束时间，并且设置了循环周期
            if (targetTime > endDate && nCount != 0) {
                // 重新计算分区，采用循环的方式
                targetPartition = targetPartition % nCount;
            }
            // 返回计算得到的分区
            return targetPartition;

        } catch (ParseException e) {
            // 如果发生解析异常，则抛出异常
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please check if the format satisfied.", e);
        }
    }


    /**
     *
     * @param beginValue
     * @param endValue
     * @return Integer[]
     */
    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        SimpleDateFormat format = new SimpleDateFormat(this.dateFormat);
        //将传入的字符串类型的日期格式化成Date类型的日期，使用 SimpleDateFormat 的 parse 方法
        try {
            Date begin = format.parse(beginValue);
            Date end = format.parse(endValue);
            Calendar cal = Calendar.getInstance();
            List<Integer> list = new ArrayList<>();
            //根据格式化后的日期通过calculate循环计算出所属的节点编号，并将节点编号添加到一个列表中
            while (begin.getTime() <= end.getTime()) {
                Integer nodeValue = this.calculate(format.format(begin));
                if (Collections.frequency(list, nodeValue) < 1) list.add(nodeValue);
                cal.setTime(begin);
                cal.add(Calendar.DATE, 1);
                begin = cal.getTime();
            }
            //最后将节点编号列表转换为数组并返回
            Integer[] nodeArray = new Integer[list.size()];
            for (int i = 0; i < list.size(); i++) {
                nodeArray[i] = list.get(i);
            }

            return nodeArray;
        } catch (ParseException e) {
            LOGGER.info("error", e);
            return new Integer[0];
        }
    }

    @Override
    public int getPartitionNum() {
        int count = this.nCount;
        return count > 0 ? count : -1;
    }

    public void setsBeginDate(String sBeginDate) {
        this.sBeginDate = sBeginDate;
        propertiesMap.put("sBeginDate", sBeginDate);
    }

    public void setsPartionDay(String sPartionDay) {
        this.sPartionDay = sPartionDay;
        propertiesMap.put("sPartionDay", sPartionDay);
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
        propertiesMap.put("dateFormat", dateFormat);
    }

    public void setsEndDate(String sEndDate) {
        this.sEndDate = sEndDate;
        propertiesMap.put("sEndDate", sEndDate);
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
        propertiesMap.put("defaultNode", String.valueOf(defaultNode));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionByDate other = (PartitionByDate) o;

        return StringUtil.equals(other.sBeginDate, sBeginDate) &&
                StringUtil.equals(other.sPartionDay, sPartionDay) &&
                StringUtil.equals(other.dateFormat, dateFormat) &&
                StringUtil.equals(other.sEndDate, sEndDate) &&
                other.defaultNode == defaultNode;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private void initHashCode() {
        long tmpCode = beginDate;
        tmpCode *= partitionTime;
        if (defaultNode != 0) {
            tmpCode *= defaultNode;
        }
        if (!StringUtil.isEmpty(sEndDate)) {
            tmpCode *= endDate;
        }
        hashCode = (int) tmpCode;
    }
}
