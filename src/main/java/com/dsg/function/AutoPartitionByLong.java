/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.dsg.function;


import com.dsg.rule.RuleAlgorithm;
import com.dsg.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 * 通过长度进行分区的方法
 * 1.分区方式由自定义的配置文件来决定，setMapFile 必配项
 * 2.设置默认节点，setDefaultNode 可选项 默认节点为-1，设置的节点需要 >= 0 或者为 -1
 * @author wuzhi
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private static final long serialVersionUID = 5752372920655270639L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoPartitionByLong.class);
    //文件路径
    private String mapFile;
    //保存根据配置文件生成的数组
    // [{valueStart,valueEnd,nodeIndex,hashCode},{valueStart,valueEnd,nodeIndex,hashCode}......]
    private LongRange[] longRanges;
    //默认节点
    private int defaultNode = -1;
    private int hashCode = 1;


    //初始化AutoPartitionByLong类并设置哈希码
    @Override
    public void init() {
        initialize();
        initHashCode();
    }

    @Override
    public void selfCheck() {

    }

    //读取映射文件并初始化长范围
    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }


    /**
     * 计算给定列值的分区索引
     * @param columnValue
     * @return 值对应的分区值
     * 1.判断传入的参数是否为null或者为NULL，如果设置了默认节点且>=0,则返回设置的节点,反之则返回null
     * 2.将传入的参数与根据配置文件生成的数组中的每个值进行判断，如果在给定的范围内，则返回此时数组值的节点索引
     *   反之如果没有匹配到，则根据默认节点的设置返回默认节点或则null
     * 3.传入的参数不符合规范，为非数字型字符串时，提示报错
     */
    @Override
    public Integer calculate(String columnValue) {
        //columnValue = NumberParseUtil.eliminateQuote(columnValue);
        try {

            if (columnValue == null || columnValue.equalsIgnoreCase("NULL")) {
                if (defaultNode >= 0) {
                    return defaultNode;
                }
                return null;
            }

            long value = Long.parseLong(columnValue);
            //将传入的参数和根据配置文件生成的数组中的每个值进行判断
            for (LongRange longRang : this.longRanges) {
                if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
                    return longRang.getNodeIndex();
                }
            }
            // use default node for other value
            if (defaultNode >= 0) {
                return defaultNode;
            }
            return null;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
    }

    /**
     * 判断是否使用默认节点
     * @param columnValue
     * @return
     * 1.检查是否应该使用默认节点
     * 2.如果在给定的范围内，返回false不使用默认节点
     *   反之会判断默认节点值是否>=0，如果是则返回true，使用默认节点，反之不使用，返回false
     */
    public boolean isUseDefaultNode(String columnValue) {
        try {
            long value = Long.parseLong(columnValue);
            for (LongRange longRang : this.longRanges) {
                if (value <= longRang.getValueEnd() && value >= longRang.getValueStart()) {
                    return false;
                }
            }
            if (defaultNode >= 0) {
                return true;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("columnValue:" + columnValue + " Please eliminate any quote and non number within it.", e);
        }
        return false;
    }


    /**
     * 计算给定起始和结束值的分区索引
     * @param beginValue 起始值
     * @param endValue 结束值
     * @return 对应节点索引组成的数组
     * 1.先判断beginValue和endValue是否需要使用默认节点，来给局部变量begin和end进行相关计算赋值
     * 2.判断计算出来的begin和end是否为null，为true则返回一个integer类型的空数组
     *   反之则将end和begin值进行比较运算来，计算出对应的integer类型数组
     */
    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        Integer begin = 0, end = 0;
        if (isUseDefaultNode(beginValue) || isUseDefaultNode(endValue)) {
            begin = 0;
            end = longRanges.length - 1;
        } else {
            begin = calculate(beginValue);
            end = calculate(endValue);
        }


        if (begin == null || end == null) {
            return new Integer[0];
        }
        if (end >= begin) {
            int len = end - begin + 1;
            Integer[] re = new Integer[len];

            for (int i = 0; i < len; i++) {
                re[i] = begin + i;
            }
            return re;
        } else {
            return new Integer[0];
        }
    }

    /**
     * 返回根据配置文件生成的数组长度
     * @return
     */
    @Override
    public int getPartitionNum() {
        return longRanges.length;
    }

    /**
     * 初始化方法
     * 1.获取配置文件
     * 2.根据配置文件配置的start-end=nodeIndex,将三个值保存到longRanges数组中
     * 3..将配置文件信息格式化，全部保存到map中
     */
    private void initialize() {
        StringBuilder sb = new StringBuilder("{");
        BufferedReader in = null;
        try {
            // FileInputStream fin = new FileInputStream(new File(fileMapPath));
            InputStream fin = ResourceUtil.getResourceAsStreamFromRoot(mapFile);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file " + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));
            //创建链表
            LinkedList<LongRange> longRangeList = new LinkedList<>();
            int iRow = 0;
            for (String line = null; (line = in.readLine()) != null; ) {
                line = line.trim();
                if ((line.length() == 0) || line.startsWith("#") || line.startsWith("//")) {
                    continue;
                }
                int ind = line.indexOf('=');
                if (ind < 0) {
                    LOGGER.info(" warn: bad line int " + mapFile + " :" + line);
                    continue;
                }

                String key = line.substring(0, ind).trim();
                String[] pairs = key.split("-");
                long longStart = NumberParseUtil.parseLong(pairs[0].trim());
                long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
                String value = line.substring(ind + 1).trim();
                int nodeId = Integer.parseInt(value);
                longRangeList.add(new LongRange(nodeId, longStart, longEnd));
                if (iRow > 0) {
                    sb.append(",");
                }
                iRow++;
                sb.append("\"");
                sb.append(key);
                sb.append("\":");
                sb.append("\"");
                sb.append(value);
                sb.append("\"");
            }
            longRanges = longRangeList.toArray(new LongRange[longRangeList.size()]);
            sb.append("}");
            propertiesMap.put("mapFile", sb.toString());
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }

        } finally {
            try {
                in.close();
            } catch (Exception e2) {
                //ignore error
            }
        }
    }

    /**
     * 设置默认节点，节点设置需要 >=0 或者 使用默认节点-1，节点会和配置文件信息一起保存到map中
     * @param defaultNode
     */
    public void setDefaultNode(int defaultNode) {
        if (defaultNode >= 0 || defaultNode == -1) {
            this.defaultNode = defaultNode;
        } else {
            LOGGER.warn("numberrange algorithm default node less than 0 and is not -1, use -1 replaced.");
        }
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
        AutoPartitionByLong other = (AutoPartitionByLong) o;
        if (other.defaultNode != defaultNode) {
            return false;
        }
        if (other.longRanges.length != longRanges.length) {
            return false;
        }
        for (int i = 0; i < longRanges.length; i++) {
            if (!other.longRanges[i].equals(longRanges[i])) {
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
        if (defaultNode != 0) {
            hashCode *= defaultNode;
        }
        for (LongRange longRange : longRanges) {
            hashCode *= longRange.hashCode();
        }
    }
}
