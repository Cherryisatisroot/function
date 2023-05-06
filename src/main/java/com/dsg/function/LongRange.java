/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.dsg.function;

import java.io.Serializable;


/**
 * 首先，将 hashCode 初始化为 1。
 *
 * 如果 nodeIndex 不等于 0，则将 hashCode 乘以 nodeIndex。
 *
 * 如果 valueEnd 和 valueStart 不相等，则将 hashCode 乘以 (valueEnd - valueStart) 的值。
 *
 * 最后，返回 hashCode 的值。
 *
 * 在这个实现中，hashCode 的计算使用了多个成员变量，并且每个成员变量都参与了计算。这种实现方式可以使得 hashCode 更加具有随机性，从而降低哈希冲突的概率。
 */
public class LongRange implements Serializable {
    private final int nodeIndex;
    private final long valueStart;
    private final long valueEnd;
    private int hashCode = 1;

    public LongRange(int nodeIndex, long valueStart, long valueEnd) {
        super();
        this.nodeIndex = nodeIndex;
        this.valueStart = valueStart;
        this.valueEnd = valueEnd;
        if (nodeIndex != 0) {
            hashCode *= nodeIndex;
        }
        if (valueEnd - valueStart != 0) {
            hashCode *= (int) (valueEnd - valueStart);
        }
    }

    public int getNodeIndex() {
        return nodeIndex;
    }

    public long getValueStart() {
        return valueStart;
    }

    public long getValueEnd() {
        return valueEnd;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongRange other = (LongRange) o;
        return other.nodeIndex == nodeIndex &&
                other.valueStart == valueStart &&
                other.valueEnd == valueEnd;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
