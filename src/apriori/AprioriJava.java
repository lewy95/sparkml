package apriori;

import java.util.*;

/**
 *
 * source:
 * cover from https://blog.csdn.net/yangmuted/article/details/48156995
 * data:
 * {牛奶，面包}
 * {面包，尿布，啤酒，鸡蛋}
 * {牛奶，尿布，啤酒，可乐}
 * {面包，牛奶，尿布，啤酒}
 * {面包，牛奶，尿布，可乐}
 * element:
 * 1.项集：项的集合称为项集，包含k个项的项集称为k-项集
 * 2.支持度：support(X–>Y) = |X交Y|/N=集合X与Y中的项在一条记录中同时出现的次数/数据记录的次数
 *          例如：support({啤酒}–>{尿布}) = 啤酒和尿布同时出现的次数/数据记录数 = 3/5=60%
 * 3.confidence(X–>Y) = |X交Y|/|X| = 集合X与集合Y中的项在一条记录中同时出现的次数/集合X出现的次数，也可理解为包含集合X的项集中集合Y出现的比例 。
 *          例如：confidence({啤酒}–>{尿布}) = 啤酒和尿布同时出现的次数/啤酒出现的次数=3/3=100%
 *          confidence({尿布}–>{啤酒}) = 啤酒和尿布同时出现的次数/尿布出现的次数 = 3/4 = 75%。
 * 4.频繁项集(frequentCollection)：如果一个项集在原始数据中出现的次数或频率满足最小支持度，则称这个项集为频繁项集。
 */
public class AprioriJava {

    private final static int SUPPORT = 2; // 支持度阈值（最小支持度）
    private final static double CONFIDENCE = 0.6; // 置信度阈值（最小支持度）

    private final static String ITEM_SPLIT = ";"; // 项之间的分隔符
    private final static String CON = "->"; // 项之间的分隔符

    private final static List<String> transList = new ArrayList<String>(); // 所有交易

    static {// 初始化交易记录
        transList.add("muller;lisa;");
        transList.add("lewy;anna;krara;");
        transList.add("toliso;thiago;javi;");
        transList.add("sergi;thiago;comman;");
        transList.add("lewy;sergi;comman;");
        transList.add("sergi;comman;");
        transList.add("comman;muller;sergi;");
        transList.add("lewy;muller;thiago;");
        transList.add("lisa;anna;klara;");
        transList.add("lisa;anna;");
        transList.add("lewy;anna;");
        transList.add("muller;comman;javi;");
        transList.add("muller;comman;toliso;");
        transList.add("muller;comman;toliso;leno");
        transList.add("lewy;comman;kimmich;alaba;");
    }

    /**
     * 获得频繁集
     * @return
     */
    public Map<String, Integer> getFC() {
        Map<String, Integer> frequentCollectionMap = new HashMap<String, Integer>();// 所有的频繁集

        frequentCollectionMap.putAll(getItem1FC());

        Map<String, Integer> itemkFcMap = new HashMap<String, Integer>();
        itemkFcMap.putAll(getItem1FC()); // 1频繁项集

        while (itemkFcMap != null && itemkFcMap.size() != 0) {
            Map<String, Integer> candidateCollection = getCandidateCollection(itemkFcMap);
            Set<String> ccKeySet = candidateCollection.keySet();

            // 对候选集项进行累加计数
            for (String trans : transList) {
                for (String candidate : ccKeySet) {
                    boolean flag = true;// 用来判断交易中是否出现该候选项，如果出现，计数加1
                    String[] candidateItems = candidate.split(ITEM_SPLIT);
                    for (String candidateItem : candidateItems) {
                        if (trans.indexOf(candidateItem + ITEM_SPLIT) == -1) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag) {
                        Integer count = candidateCollection.get(candidate);
                        candidateCollection.put(candidate, count + 1);
                    }
                }
            }

            // 从候选集中找到符合支持度的频繁集项
            itemkFcMap.clear();
            for (String candidate : ccKeySet) {
                Integer count = candidateCollection.get(candidate);
                if (count >= SUPPORT) {
                    itemkFcMap.put(candidate, count);
                }
            }

            // 合并所有频繁集
            frequentCollectionMap.putAll(itemkFcMap);

        } // while

        return frequentCollectionMap;
    }// getFC

    /**
     * 不断迭代使用频繁k-项集去产生频繁k+1项集
     * @param itemkFcMap
     * @return
     */
    private Map<String, Integer> getCandidateCollection(Map<String, Integer> itemkFcMap) {
        Map<String, Integer> candidateCollection = new HashMap<String, Integer>();
        Set<String> itemkSet1 = itemkFcMap.keySet();
        Set<String> itemkSet2 = itemkFcMap.keySet();

        for (String itemk1 : itemkSet1) {
            for (String itemk2 : itemkSet2) {
                // 进行连接
                String[] tmp1 = itemk1.split(ITEM_SPLIT);
                String[] tmp2 = itemk2.split(ITEM_SPLIT);

                String c = "";
                // 1项集
                if (tmp1.length == 1) {
                    // 只存储增序
                    if (tmp1[0].compareTo(tmp2[0]) < 0) {
                        c = tmp1[0] + ITEM_SPLIT + tmp2[0] + ITEM_SPLIT; // 1;2; 这种形式
                    }
                }
                // 多项集
                else {
                    boolean flag = true;
                    for (int i = 0; i < tmp1.length - 1; i++) {
                        if (!tmp1[i].equals(tmp2[i])) {
                            flag = false;
                            break;
                        }
                    }
                    if (flag && (tmp1[tmp1.length - 1].compareTo(tmp2[tmp2.length - 1]) < 0)) {
                        c = itemk1 + tmp2[tmp2.length - 1] + ITEM_SPLIT;
                    }
                }

                // 进行剪枝
                boolean hasInfrequentSubSet = false;
                if (!c.equals("")) {
                    String[] tmpC = c.split(ITEM_SPLIT);
                    for (int i = 0; i < tmpC.length; i++) {
                        String subC = "";
                        for (int j = 0; j < tmpC.length; j++) {
                            if (i != j) {
                                subC = subC + tmpC[j] + ITEM_SPLIT;
                            }
                        }
                        if (itemkFcMap.get(subC) == null) {
                            hasInfrequentSubSet = true;
                            break;
                        }
                    }
                } else {
                    hasInfrequentSubSet = true;
                }

                if (!hasInfrequentSubSet) {
                    candidateCollection.put(c, 0);
                }
            }
        }

        return candidateCollection;
    }

    /**
     * 1-frequent itemset
     * 获取一级候选项集
     * @return
     */
    private Map<String, Integer> getItem1FC() {
        Map<String, Integer> sItem1FcMap = new HashMap<String, Integer>();
        Map<String, Integer> rItem1FcMap = new HashMap<String, Integer>();// 频繁1项集

        //统计一级候选项集出现次数，本质wordcount
        for (String trans : transList) {
            String[] items = trans.split(ITEM_SPLIT);
            for (String item : items) {
                Integer count = sItem1FcMap.get(item + ITEM_SPLIT);
                if (count == null) {
                    sItem1FcMap.put(item + ITEM_SPLIT, 1);
                } else {
                    sItem1FcMap.put(item + ITEM_SPLIT, count + 1);
                }
            }
        }

        //消除不满足条件（出现次数必须大于最小支持度）的项集
        Set<String> keySet = sItem1FcMap.keySet();
        for (String key : keySet) {
            Integer count = sItem1FcMap.get(key);
            if (count >= SUPPORT) {
                rItem1FcMap.put(key, count);
            }
        }
        return rItem1FcMap;
    }

    public Map<String, Double> getRelationRules(Map<String, Integer> frequentCollectionMap) {
        Map<String, Double> relationRules = new HashMap<String, Double>();
        Set<String> keySet = frequentCollectionMap.keySet();
        for (String key : keySet) {
            double countAll = frequentCollectionMap.get(key);
            String[] keyItems = key.split(ITEM_SPLIT);
            if (keyItems.length > 1) {
                List<String> source = new ArrayList<String>();
                Collections.addAll(source, keyItems);
                List<List<String>> result = new ArrayList<List<String>>();

                buildSubSet(source, result);// 获得source的所有非空子集

                for (List<String> itemList : result) {
                    if (itemList.size() < source.size()) {// 只处理真子集
                        List<String> otherList = new ArrayList<String>();
                        for (String sourceItem : source) {
                            if (!itemList.contains(sourceItem)) {
                                otherList.add(sourceItem);
                            }
                        }
                        String reasonStr = "";// 前置
                        String resultStr = "";// 结果
                        for (String item : itemList) {
                            reasonStr = reasonStr + item + ITEM_SPLIT;
                        }
                        for (String item : otherList) {
                            resultStr = resultStr + item + ITEM_SPLIT;
                        }

                        double countReason = frequentCollectionMap.get(reasonStr);
                        double itemConfidence = countAll / countReason;// 计算置信度
                        if (itemConfidence >= CONFIDENCE) {
                            String rule = reasonStr + CON + resultStr;
                            relationRules.put(rule, itemConfidence);
                        }
                    }
                }
            }
        }

        return relationRules;
    }

    private void buildSubSet(List<String> sourceSet, List<List<String>> result) {
        // 仅有一个元素时，递归终止。此时非空子集仅为其自身，所以直接添加到result中
        if (sourceSet.size() == 1) {
            List<String> set = new ArrayList<String>();
            set.add(sourceSet.get(0));
            result.add(set);
        } else if (sourceSet.size() > 1) {
            // 当有n个元素时，递归求出前n-1个子集，在于result中
            buildSubSet(sourceSet.subList(0, sourceSet.size() - 1), result);
            int size = result.size();// 求出此时result的长度，用于后面的追加第n个元素时计数
            // 把第n个元素加入到集合中
            List<String> single = new ArrayList<String>();
            single.add(sourceSet.get(sourceSet.size() - 1));
            result.add(single);
            // 在保留前面的n-1子集的情况下，把第n个元素分别加到前n个子集中，并把新的集加入到result中;
            // 为保留原有n-1的子集，所以需要先对其进行复制
            List<String> clone;
            for (int i = 0; i < size; i++) {
                clone = new ArrayList<String>();
                for (String str : result.get(i)) {
                    clone.add(str);
                }
                clone.add(sourceSet.get(sourceSet.size() - 1));

                result.add(clone);
            }
        }
    }

    public static void main(String[] args) {
        AprioriJava apriori = new AprioriJava();
        Map<String, Integer> frequentCollectionMap = apriori.getFC();
        System.out.println("----------------频繁集" + "----------------");
        Set<String> fcKeySet = frequentCollectionMap.keySet();
        for (String fcKey : fcKeySet) {
            System.out.println(fcKey + "  :  " + frequentCollectionMap.get(fcKey));
        }
        Map<String, Double> relationRulesMap = apriori.getRelationRules(frequentCollectionMap);
        System.out.println("----------------关联规则" + "----------------");
        Set<String> rrKeySet = relationRulesMap.keySet();
        for (String rrKey : rrKeySet) {
            System.out.println(rrKey + "  :  " + relationRulesMap.get(rrKey));
        }
    }
}

