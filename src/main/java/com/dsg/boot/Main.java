package com.dsg.boot;

import com.dsg.function.*;
import com.dsg.util.PartitionUtil;
import com.dsg.util.SplitUtil;

import java.util.Scanner;

/**
 * @author Lemonisatisroot
 * @title Main
 * @date 2023/5/4 11:15
 * @description TODO
 */
public class Main {

    private static String mapFile;
    private static int node = -1;
    private static Boolean isSetNode = false;
    private static int method;
    private static boolean isNext;
//    private static int [] count;
//    private static int [] length;

    public static void main(String[] args) {

        for(;;) {

            isNext = true;

            System.out.println("----------------------------------------------");
            System.out.println("--------1.AutoPartitionByLong-----------------");
            System.out.println("--------2.PartitionByDate---------------------");
            System.out.println("--------3.PartitionByFileMap------------------");
            System.out.println("--------4.PartitionByLong---------------------");
            System.out.println("--------5.PartitionByPattern------------------");
            System.out.println("--------6.PartitionByString-------------------");
            System.out.println("--------7.exit--------------------------------");
            Scanner scanner = new Scanner(System.in);
            System.out.print("选择: ");
            int choose = scanner.nextInt();
            switch (choose) {
                case 1:
                    System.out.print("请输入配置文件(autopartition-long.txt):");
                    mapFile = scanner.next();
                    System.out.println(mapFile);
                    System.out.print("是否设置默认节点(true/false): ");
                    isSetNode = scanner.nextBoolean();
                    if (isSetNode) {
                        System.out.print("请设置节点:");
                        node = scanner.nextInt();
                    }
                    System.out.println("初始化中......");
                    //"autopartition-long.txt"
                    AutoPartitionByLong partition = initPartition(mapFile, node);
                    System.out.println("初始化完成 ---> " + partition.getAllProperties());
                    isNext = true;
                    while (isNext) {
                        System.out.println("选择方法 1:calcuate(String str) 2:calcuateRange(String str1, String str2) 3:退出: ");
                        method = scanner.nextInt();
                        switch (method) {
                            case 1:
                                System.out.println("设置参数: ");
                                String value = scanner.next();
                                Integer integer = partition.calculate(value);
                                System.out.println("返回值为 ---> " + integer);
                                break;
                            case 2:
                                System.out.println("设置参数1: ");
                                String start = scanner.next();
                                System.out.println("设置参数2: ");
                                String end = scanner.next();
                                Integer[] integers = partition.calculateRange(start, end);
                                System.out.println("返回的数组长度为 ---> " + integers.length);
                                System.out.print("具体值为 ---> ");
                                for (Integer result : integers) {
                                    System.out.print( result + " ");
                                }
                                System.out.println();
                                break;
                            default:
                                isNext = false;
                                break;
                        }
                    }
                    break;
                case 2:
                    System.out.print("格式化时间(yyyy-MM-dd,yyyy/MM/dd......): ");
                    String dateFormat = scanner.next();
                    System.out.println("设置起始时间: ");
                    String beginDate = scanner.next();
                    System.out.println("是否设置结束时间(true/false): ");
                    Boolean isSetEnd = scanner.nextBoolean();
                    String endDate = null;
                    if (isSetEnd) {
                        System.out.println("设置结束时间: ");
                        endDate = scanner.next();
                    }
                    System.out.println("设置分区方式(多少天为一个分区): ");
                    String partitionDay = scanner.next();
                    System.out.print("是否设置默认节点(true/false): ");
                    isSetNode = scanner.nextBoolean();
                    if (isSetNode) {
                        System.out.print("请设置节点:");
                        node = scanner.nextInt();
                    }
                    System.out.println("初始化中......");
                    PartitionByDate partitionByDate = initPartitionDate(dateFormat, beginDate, partitionDay, endDate, node);
                    System.out.println("初始化完成......");

                    while (isNext) {
                        System.out.println("选择方法 1:calcuate(String str) 2:calcuateRange(String str1, String str2) 3:退出: ");
                        method = scanner.nextInt();
                        switch (method) {
                            case 1:
                                System.out.println("设置参数: ");
                                String value = scanner.next();
                                Integer integer = partitionByDate.calculate(value);
                                System.out.println("返回值为 ---> " + integer);
                                break;
                            case 2:
                                System.out.println("设置参数1: ");
                                String start = scanner.next();
                                System.out.println("设置参数2: ");
                                String end = scanner.next();
                                Integer[] integers = partitionByDate.calculateRange(start, end);
                                System.out.println("返回的数组长度为 ---> " + integers.length);
                                System.out.print("具体值为 ---> ");
                                for (Integer result : integers) {
                                    System.out.print(result + " ");
                                }
                                System.out.println();
                                break;
                            default:
                                isNext = false;
                                break;
                        }
                    }
                    break;
                case 3:
                    System.out.print("请输入配置文件(partition-hash-int.txt):");
                    mapFile = scanner.next();
                    System.out.println(mapFile);
                    System.out.print("是否设置默认节点(true/false): ");
                    isSetNode = scanner.nextBoolean();
                    if (isSetNode) {
                        System.out.print("请设置节点:");
                        node = scanner.nextInt();
                    }
                    System.out.print("是否设置默认类型(true/false): ");
                    boolean isSetType = scanner.nextBoolean();
                    int type = -1;
                    if (isSetType) {
                        System.out.print("请设置类型:");
                        type = scanner.nextInt();
                    }
                    System.out.println("初始化中......");
                    //partition-hash-int.txt
                    PartitionByFileMap partitionByFileMap = initPartitionFileMap(mapFile, node, type);
                    System.out.println("初始化完成 ---> " + partitionByFileMap.getAllProperties());

                    while (isNext) {
                        System.out.println("选择方法 1:calcuate(String str) 2:calcuateRange(String str1, String str2) 3:退出: ");
                        method = scanner.nextInt();
                        switch (method) {
                            case 1:
                                System.out.println("设置参数: ");
                                String value = scanner.next();
                                Integer integer = partitionByFileMap.calculate(value);
                                System.out.println("返回值为 ---> " + integer);
                                break;
                            case 2:
                                System.out.println("设置参数1: ");
                                String start = scanner.next();
                                System.out.println("设置参数2: ");
                                String end = scanner.next();
                                Integer[] integers = partitionByFileMap.calculateRange(start, end);
                                System.out.println("返回的数组长度为 ---> " + integers.length);
                                System.out.print("具体值为 ---> ");
                                for (Integer result : integers) {
                                    System.out.print(result + " ");
                                }
                                System.out.println();
                                break;
                            default:
                                isNext = false;
                                break;
                        }
                    }
                    break;
                case 4:
                    System.out.print("设置分区长度(): ");
                    String partitionLength = scanner.next();
                    System.out.println("设置分区总数: ");
                    String partitionCount = scanner.next();
                    System.out.println("初始化中......");
                    PartitionByLong partitionByLong = initPartitionLong(partitionLength, partitionCount);
                    System.out.println("初始化完成 ---> " + partitionByLong.getAllProperties());
                    while (isNext) {
                        System.out.println("选择方法 1:calcuate(String str) 2:calcuateRange(String str1, String str2) 3:退出: ");
                        method = scanner.nextInt();
                        switch (method) {
                            case 1:
                                System.out.println("设置参数: ");
                                String value = scanner.next();
                                Integer integer = partitionByLong.calculate(value);
                                System.out.println("返回值为 ---> " + integer);
                                break;
                            case 2:
                                System.out.println("设置参数1: ");
                                String start = scanner.next();
                                System.out.println("设置参数2: ");
                                String end = scanner.next();
                                Integer[] integers = partitionByLong.calculateRange(start, end);
                                System.out.println("返回的数组长度为 ---> " + integers.length);
                                System.out.print("具体值为 ---> ");
                                for (Integer result : integers) {
                                    System.out.print(result + " ");
                                }
                                System.out.println();
                                break;
                            default:
                                isNext = false;
                                break;
                        }
                    }
                    break;
                case 5:
                    System.out.print("请输入配置文件(partition-pattern.txt):");
                    mapFile = scanner.next();
                    System.out.println(mapFile);
                    System.out.print("是否设置默认节点(true/false): ");
                    isSetNode = scanner.nextBoolean();
                    if (isSetNode) {
                        System.out.print("请设置节点:");
                        node = scanner.nextInt();
                    }
                    System.out.println("设置模式值: ");
                    int patternValue = scanner.nextInt();
                    System.out.println("初始化中......");
                    //partition-pattern.txt
                    PartitionByPattern partitionByPattern = initPartitionPattern(mapFile, patternValue, node);
                    System.out.println("初始化完成 ---> " + partitionByPattern.getAllProperties());
                    while (isNext) {
                        System.out.println("选择方法 1:calcuate(String str) 2:calcuateRange(String str1, String str2) 3:退出: ");
                        method = scanner.nextInt();
                        switch (method) {
                            case 1:
                                System.out.println("设置参数: ");
                                String value = scanner.next();
                                Integer integer = partitionByPattern.calculate(value);
                                System.out.println("返回值为 ---> " + integer);
                                break;
                            case 2:
                                System.out.println("设置参数1: ");
                                String start = scanner.next();
                                System.out.println("设置参数2: ");
                                String end = scanner.next();
                                Integer[] integers = partitionByPattern.calculateRange(start, end);
                                System.out.println("返回的数组长度为 ---> " + integers.length);
                                System.out.print("具体值为 ---> ");
                                for (Integer result : integers) {
                                    System.out.print(result + " ");
                                }
                                System.out.println();
                                break;
                            default:
                                isNext = false;
                                break;
                        }
                    }
                    break;
                case 6:
                    System.out.println("设置分区长度(例如 5 or 4,5): ");
                    String length = scanner.next();
                    System.out.println("设置分区总数(例如 5 or 4,5): ");
                    String count = scanner.next();
                    System.out.println("初始化中......");
                    PartitionByString partitionByString = initPartitionString(length, count);
                    System.out.println("初始化完成 ---> " + partitionByString.getAllProperties());
                    System.out.println("设置起始值(0:2): ");
                    String hashSlice = scanner.next();
                    partitionByString.setHashSlice(hashSlice);
                    while (isNext) {
                        System.out.println("选择方法 1:calcuate(String str) 2:calcuateRange(String str1, String str2) 3:退出: ");
                        method = scanner.nextInt();
                        switch (method) {
                            case 1:
                                System.out.println("设置参数: ");
                                String value = scanner.next();
                                Integer integer = partitionByString.calculate(value);
                                System.out.println("返回值为 ---> " + integer);
                                break;
                            case 2:
                                System.out.println("设置参数1: ");
                                String start = scanner.next();
                                System.out.println("设置参数2: ");
                                String end = scanner.next();
                                Integer[] integers = partitionByString.calculateRange(start, end);
                                System.out.println("返回的数组长度为 ---> " + integers.length);
                                System.out.print("具体值为 ---> ");
                                for (Integer result : integers) {
                                    System.out.print(result + " ");
                                }
                                System.out.println();
                                break;
                            default:
                                isNext = false;
                                break;
                        }
                    }
                    break;
                default:
                    System.out.println("退出......");
                    return;
            }

        }

    }


    private static AutoPartitionByLong initPartition(String mapFile, Integer defaultNode) {
        AutoPartitionByLong partitionByLong = new AutoPartitionByLong();
        partitionByLong.setMapFile(mapFile);
        partitionByLong.setDefaultNode(defaultNode);
        partitionByLong.init();
        return partitionByLong;
    }

    private static PartitionByDate initPartitionDate(String dateFormat, String beginDate, String partionDay, String endDate, Integer node) {
        PartitionByDate partitionByDate = new PartitionByDate();
        partitionByDate.setDateFormat(dateFormat);
        partitionByDate.setsBeginDate(beginDate);
        partitionByDate.setsPartionDay(partionDay);
        partitionByDate.setsEndDate(endDate);
        partitionByDate.setDefaultNode(node);
        partitionByDate.init();
        return partitionByDate;
    }

    private static PartitionByFileMap initPartitionFileMap(String mapFile, Integer node, Integer type) {
        PartitionByFileMap partitionByFileMap = new PartitionByFileMap();
        partitionByFileMap.setMapFile(mapFile);
        partitionByFileMap.setDefaultNode(node);
        partitionByFileMap.setType(type);
        partitionByFileMap.init();
        return partitionByFileMap;
    }

    private static PartitionByLong initPartitionLong(String partitionLength, String partitionCount) {
        PartitionByLong partitionByLong = new PartitionByLong();
        partitionByLong.setPartitionCount(partitionCount);
        partitionByLong.setPartitionLength(partitionLength);
        partitionByLong.init();
        return partitionByLong;
    }

    private static PartitionByPattern initPartitionPattern(String mapFile, Integer patternValue, Integer node) {
        PartitionByPattern partitionByPattern = new PartitionByPattern();
        partitionByPattern.setPatternValue(patternValue);
        partitionByPattern.setMapFile(mapFile);
        partitionByPattern.setDefaultNode(node);
        partitionByPattern.init();
        return partitionByPattern;
    }

    private static PartitionByString initPartitionString(String partitionLength, String partitionCount) {
        PartitionByString partitionByString = new PartitionByString();
        partitionByString.setPartitionLength(partitionLength);
        partitionByString.setPartitionCount(partitionCount);
        partitionByString.init();
        return partitionByString;
    }
}
