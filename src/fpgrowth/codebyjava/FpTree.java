package fpgrowth.codebyjava;

import java.util.ArrayList;
import java.util.List;

public class FpTree {

    String idName;// id号
    List<FpTree> children;// 孩子结点
    FpTree parent;// 父结点
    FpTree next;// 下一个id号相同的结点
    long count;// 出现次数

    public FpTree() {// 用于构造根结点
        this.idName = null;
        this.count = -1;
        children = new ArrayList<FpTree>();
        next = null;
        parent = null;
    }

    /**
     * 用于构造非跟结点
     *
     * @param idName
     */
    public FpTree(String idName) {
        this.idName = idName;
        this.count = 1;
        children = new ArrayList<FpTree>();
        next = null;
        parent = null;
    }

    /**
     * 用于生成非跟结点
     *
     * @param idName
     * @param count
     */
    public FpTree(String idName, long count) {
        this.idName = idName;
        this.count = count;
        children = new ArrayList<FpTree>();
        next = null;
        parent = null;
    }

    /**
     * 添加一个孩子
     *
     * @param child
     */
    public void addChild(FpTree child) {
        children.add(child);
    }

    public void addCount(int count) {
        this.count += count;
    }

    /**
     * 计算器加1
     */
    public void addCount() {
        this.count += 1;
    }

    /**
     * 设置下一个结点
     *
     * @param next
     */
    public void setNextNode(FpTree next) {
        this.next = next;
    }

    public void setParent(FpTree parent) {
        this.parent = parent;
    }

    /**
     * 指定取孩子
     *
     * @param index
     * @return
     */
    public FpTree getChilde(int index) {
        return children.get(index);
    }

    /**
     * 查找是否包含id号为idName的孩子
     *
     * @param idName
     * @return
     */
    public int hasChild(String idName) {
        for (int i = 0; i < children.size(); i++)
            if (children.get(i).idName.equals(idName))
                return i;
        return -1;
    }

    public String toString() {
        return "id: " + idName + " count: " + count + " 孩子个数 "
                + children.size();
    }

}
