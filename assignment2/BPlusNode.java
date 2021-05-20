

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BPlusNode<K extends Comparable<K>, V> {
    /** key list entries of current node */
    private final List<Map.Entry<K, V>> entryList;
    /** child node list of current node */
    private List<BPlusNode<K, V>> childrenList;

    /** is it a leaf node */
    private final boolean leaf;
    /** parent node */
    private BPlusNode<K, V> parent;
    /** previous node of leaf node */
    private BPlusNode<K, V> prev;
    /** next node of leaf node */
    private BPlusNode<K, V> next;


    public BPlusNode(boolean leaf) {
        if (!leaf) {
            childrenList = new ArrayList<>();
        }
        this.leaf = leaf;
        entryList = new ArrayList<>();
    }

    /**
     * get the matched value by key
     * @param key key
     * @return the matched value
     */
    public V find(K key) {
        //if it's a leaf node
        if (leaf) {
            return binarySearchOnLeafNode(key);
        }
        // If it's not a leaf node
        // If the key is less than the leftmost key of the node, continue searching along the first child node
        if (key.compareTo(entryList.get(0).getKey()) < 0) {
            return childrenList.get(0).find(key);
            // If the key is greater than or equal to the rightmost key of the node, continue searching along the last child node
        } else if (key.compareTo(entryList.get(entryList.size() - 1).getKey()) >= 0) {
            return childrenList.get(childrenList.size() - 1).find(key);
        } else {
            // Otherwise, continue the search along the previous child node greater than the key
            return binarySearchOnNode(key);
        }
    }

    /**
     * Binary search along internal nodes
     * @param key search key
     * @return matched value
     */
    protected V binarySearchOnNode(K key){
        int low = 0, high = entryList.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            int res = key.compareTo(entryList.get(mid).getKey());
            if (res == 0) {
                return childrenList.get(mid + 1).find(key);
            } else if (res < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return childrenList.get(low).find(key);
    }

    /**
     * handle the linked list relationship of leaf nodes
     * @param left left node
     * @param right right node
     * @param tree B+ tree
     */
    protected void handleLinkedList(BPlusNode<K,V> left,BPlusNode<K,V> right,BPlusTree<K, V> tree){
        if (next != null) {
            next.prev = right;
            right.next = next;
        }

        if (prev == null) {
            tree.setLeafHead(left);
        } else {
            prev.next = left;
            left.prev = prev;
        }

        left.next = right;
        right.prev = left;

    }

    protected void adjustParentAndChildren(int entryIdx, BPlusNode<K,V> left,BPlusNode<K,V> right,BPlusTree<K,V> tree){
        //If it's not root node
        if (parent != null) {
            left.parent = parent;
            right.parent = parent;
            int index = parent.childrenList.indexOf(this);
            if(entryIdx == 0){
                parent.entryList.add(index, right.entryList.get(entryIdx));
            }else{
                parent.entryList.add(index, entryList.get(entryIdx));
            }
            parent.childrenList.remove(this);
            parent.childrenList.add(index, left);
            parent.childrenList.add(index + 1, right);
            // Parent node insert or update key
            parent.updateInsert(tree);

        //else it's root node
        } else {
            BPlusNode<K, V> parent = new BPlusNode<>(false);
            tree.setRootNode(parent);
            tree.setHeight(tree.getHeight() + 1);
            left.parent = parent;
            right.parent = parent;
            if(entryIdx == 0){
                parent.entryList.add(right.entryList.get(entryIdx));
            }else{
                parent.entryList.add(entryList.get(entryIdx));
            }
            parent.childrenList.add(left);
            parent.childrenList.add(right);
        }
    }

    /**
     * insert the new node into the B+ tree
     * if there is a node with the key, update the original node to the new node
     * @param key insert key
     * @param value insert value
     * @param tree B+ tree
     */
    public void insertOrUpdate(K key, V value, BPlusTree<K, V> tree) {
        //update height
        tree.setHeight(tree.getHeight() == 0 ? 1 : tree.getHeight());
        //If it's a leaf node
        if (leaf) {
            //If b plus tree not contains key or node entry's size is smaller than order,just insert or update
            if (contains(key) != -1 || entryList.size() < tree.getOrder()) {
                insertOrUpdate(key, value);
                return;
            }
            //else, B+ tree needs to be split into two nodes
            BPlusNode<K, V> left = new BPlusNode<>(true), right = new BPlusNode<>(true);
            //handle linkedList on leaf nodes
            handleLinkedList(left,right,tree);
            // copy the key of the original node to the split new node
            splitAndCopyNodes(tree, left, right ,key, value);
            //adjust relationship between children and parent
            adjustParentAndChildren(0,left,right,tree);
            return;
        }
        // if it's not leaf node
        // if the key is less than or equal to the leftmost key of the node, continue searching along the first child node
        if (key.compareTo(entryList.get(0).getKey()) < 0) {
            childrenList.get(0).insertOrUpdate(key, value, tree);
        } else if (key.compareTo(entryList.get(entryList.size() - 1).getKey()) >= 0) {
            // if the key is greater than the rightmost key of the node, continue searching along the last child node
            childrenList.get(childrenList.size() - 1).insertOrUpdate(key, value, tree);
        } else {
            // otherwise, continue the search along the previous child node greater than the key
            binarySearchOnInternalNode(key,value,tree);
        }
    }

    /**
     * perform a binary search at the intermediate node, find a suitable position to insert or update
     * @param key insert key
     * @param value insert value
     * @param tree B+ tree
     */
    protected void binarySearchOnInternalNode(K key, V value, BPlusTree<K,V> tree){
        int low = 0, high = entryList.size() - 1;
        while (low <= high) {
            int mid = low + (high - low ) / 2;
            int res = key.compareTo(entryList.get(mid).getKey());
            if (res == 0) {
                childrenList.get(mid + 1).insertOrUpdate(key, value, tree);
                break;
            } else if (res < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        if (low > high) {
            childrenList.get(low).insertOrUpdate(key, value, tree);
        }
    }

    /**
     *
     * @param key insert key
     * @param value insert value
     * @param left new left node
     * @param right new right node
     * @param tree B+ tree
     */
    private void splitAndCopyNodes(BPlusTree<K, V> tree, BPlusNode<K, V> left, BPlusNode<K, V> right, K key, V value ) {
        // record whether the new node has been inserted
        boolean isInserted = false;
        // calculate the key's entry list length of the left and right nodes
        int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2;
        for (int i = 0; i < entryList.size(); i++) {
            if (leftSize != 0) {
                leftSize--;
                // if the inserted value is less than the first, just add to left entry list
                if (!isInserted && entryList.get(i).getKey().compareTo(key) > 0) {
                    i--;
                    isInserted = true;
                    left.entryList.add(new AbstractMap.SimpleEntry<>(key, value));
                } else {
                    //else,move the original element to the left entry list
                    left.entryList.add(entryList.get(i));
                }
            } else {
                // if the inserted value is less than the first, just add to left entry list
                if (!isInserted && entryList.get(i).getKey().compareTo(key) > 0) {
                    i--;
                    isInserted = true;
                    right.entryList.add(new AbstractMap.SimpleEntry<>(key, value));
                } else {
                    right.entryList.add(entryList.get(i));
                }
            }
        }
        if (!isInserted) {
            right.entryList.add(new AbstractMap.SimpleEntry<>(key, value));
        }
    }

    /**
     * update the intermediate node after inserting the node
     * @param tree node
     */
    protected void updateInsert(BPlusTree<K, V> tree) {
        // if the number of child nodes exceeds the order, the node needs to be split
        if (childrenList.size() > tree.getOrder()) {
            // the node will be split into left and right node
            BPlusNode<K, V> left = new BPlusNode<>(false), right = new BPlusNode<>(false);
            // calculate the length of the child nodes of the left and right nodes
            int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2, rightSize = (tree.getOrder() + 1) / 2;
            // copy the child node to the split new node, and update the key

            for (int i = 0; i < leftSize; i++) {
                left.childrenList.add(childrenList.get(i));
                childrenList.get(i).parent = left;
                if(i < leftSize - 1){
                    left.entryList.add(entryList.get(i));
                }
            }

            for (int i = 0; i < rightSize; i++) {
                right.childrenList.add(childrenList.get(leftSize + i));
                childrenList.get(leftSize + i).parent = right;
                if(i < rightSize - 1){
                    right.entryList.add(entryList.get(leftSize + i));
                }
            }
            adjustParentAndChildren(leftSize - 1,left,right,tree);
        }
    }

    /**
     * Determine whether the current node contains the key
     * @param key key
     *@return -1 means not contains,other means contains
     */
    protected int contains(K key) {
        int low = 0, high = entryList.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            int res = key.compareTo(entryList.get(mid).getKey());
            if (res == 0) {
                return mid;
            } else if (res < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return -1;
    }

    /**
     * Binary search in leaf nodes
     * @param key search key
     * @return the matched value
     */
    protected V binarySearchOnLeafNode(K key){
        int low = 0, high = entryList.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            int res = key.compareTo(entryList.get(mid).getKey());
            if (res == 0) {
                return entryList.get(mid).getValue();
            } else if (res < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        //not found,return null
        return null;
    }

    /**
     * print B+ tree in console
     * @param index tree's level
     * @param sb StringBuilder
     */
    public void printBPlusTree(int index,StringBuilder sb) {
        if (this.leaf) {
            sb.append("Level is ").append(index).append(",leaf node，keys is ");
            for (Map.Entry<K, V> entry : entryList) sb.append(entry).append(" ");
            sb.append("\n");
        } else {
            sb.append("Level is ").append(index).append(",not leaf node，keys is ");
            for (Map.Entry<K, V> entry : entryList) sb.append(entry.getKey()).append(" ");
            sb.append("\n");
            for (BPlusNode<K, V> child : childrenList) child.printBPlusTree(index + 1,sb);
        }
    }

    /**
     * Insert the key into the current node entries
     * @param key inserted key
     * @param value inserted value
     */
    protected void insertOrUpdate(K key, V value) {
        //binary search and insert
        int low = 0, high = entryList.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            int res = key.compareTo(entryList.get(mid).getKey());
            if (res == 0) {
                entryList.get(mid).setValue(value);
                break;
            } else if (res < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        if (low > high) {
            entryList.add(low, new AbstractMap.SimpleEntry<>(key, value));
        }
    }


}