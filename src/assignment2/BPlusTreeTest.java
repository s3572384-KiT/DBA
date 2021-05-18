package assignment2;

import java.util.ArrayList;
import java.util.List;

public class BPlusTreeTest {

    public static void main(String[] args) {
        String csvFile = "count.csv";
        if(args.length > 0){
            csvFile = args[0];
        }
        //read data from csv file
        System.out.println("Reading data from csv file is started...");
        List<Count> countList = CsvUtils.readCountFromCsv(csvFile,100);
        System.out.println("Reading data from csv file is end, the rows is " + countList.size());
        //count id used to be index
        System.out.println("Creating B+ tree is started...");
        long start = System.currentTimeMillis();
        BPlusTree<Integer,Count> bPlusTree = createBPlusTreeById(countList,10);
        long end = System.currentTimeMillis();
        System.out.println("Creating B+ tree is end,the B+ tree's height is " + bPlusTree.getHeight() + ", elapsed time is " + (end - start));
        //print B+ tree
        bPlusTree.printBPlusTree();

        int searchId = 2887714;
        //search by B+ tree
        start = System.currentTimeMillis();
        Count result =  bPlusTree.get(searchId);
        end = System.currentTimeMillis();
        System.out.println("Query by B+ tree, elapsed time is " + (end - start) + " ms, result is " +result);

        //search by full scan
        start = System.currentTimeMillis();
        List<Count> resList = fullScan(countList,searchId);
        end = System.currentTimeMillis();
        System.out.println("Query by full scan, elapsed time is " + (end - start) + " ms, result size is " +resList.size());

    }

    /**
     * full scan in count list
     * @param countList count list
     * @param id search id
     * @return matched count list
     */
    public static List<Count> fullScan(List<Count> countList,int id){
        List<Count> resList = new ArrayList<>();
        for(Count count : countList){
            if(count.id == id){
                resList.add(count);
            }
        }
        return resList;
    }


    /**
     * create index in id
     * @param countList count lsit
     * @param order order of B+ Tree
     * @return B+ tree
     */
    public static BPlusTree<Integer,Count> createBPlusTreeById(List<Count> countList, int order){
        BPlusTree<Integer, Count> bPlusTree = new BPlusTree<>(order);
        countList.forEach(count -> bPlusTree.insertOrUpdate(count.id,count));
        return bPlusTree;
    }

}