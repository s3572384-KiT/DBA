

public class BPlusTree<K extends Comparable<K>, V> {
    /** root node */
    private BPlusNode<K, V> root;
    /** tree's order */
    private final int order;
    /** LinkedList node head */
    private BPlusNode<K, V> leafHead;
    /** height of tree */
    private int height = 0;

    public BPlusTree(int order) {
        if (order < 3) {
            System.out.print("The B+ tree's order must be greater than 2");
            System.exit(1);
        }
        this.order = order;
        root = new BPlusNode<>(true);
        leafHead = root;
    }


    public void setRootNode(BPlusNode<K, V> root) {
        this.root = root;
    }
    public void printBPlusTree() {
        StringBuilder sb = new StringBuilder();
        this.root.printBPlusTree(0,sb);
        System.out.print(sb);
    }
    public int getHeight() {
        return height;
    }
    public int getOrder() {
        return order;
    }
    public void setHeight(int height) {
        this.height = height;
    }
    public V find(K key) {
        return root.find(key);
    }
    public void insertOrUpdate(K key, V value) {
        root.insertOrUpdate(key, value, this);
    }
    public void setLeafHead(BPlusNode<K, V> leafHead) {
        this.leafHead = leafHead;
    }
    public BPlusNode<K,V> getLeafHead(){
        return leafHead;
    }
}