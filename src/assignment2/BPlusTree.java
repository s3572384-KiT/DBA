package assignment2;

public class BPlusTree<K extends Comparable<K>, V> {
    /** root node */
    protected BPlusNode<K, V> root;
    /** M */
    protected int order;
    /** LinkedList node head */
    protected BPlusNode<K, V> head;
    /** height of tree */
    protected int height = 0;

    public BPlusNode<K, V> getHead() {
        return head;
    }

    public void setHead(BPlusNode<K, V> head) {
        this.head = head;
    }

    public BPlusNode<K, V> getRoot() {
        return root;
    }

    public void setRoot(BPlusNode<K, V> root) {
        this.root = root;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getHeight() {
        return height;
    }

    public V get(K key) {
        return root.get(key);
    }

    public void insertOrUpdate(K key, V value) {
        root.insertOrUpdate(key, value, this);
    }

    public BPlusTree(){
        this(3);
    }

    public BPlusTree(int order) {
        if (order < 3) {
            System.out.print("The B+ tree's order must be greater than 2");
            System.exit(0);
        }
        this.order = order;
        root = new BPlusNode<>(true, true);
        head = root;
    }

    public void printBPlusTree() {
        this.root.printBPlusTree(0);
    }
}