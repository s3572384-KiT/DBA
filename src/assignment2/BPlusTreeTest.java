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
        List<Count> countList = CsvUtils.readCountFromCsv(csvFile,-1);
        System.out.println("Reading data from csv file is end, the rows is " + countList.size());
        //count id used to be index
        System.out.println("Creating BPlusTree is started...");
        long start = System.currentTimeMillis();
        BPlusTree<Integer,Count> bPlusTree = createBPlusTreeById(countList,20);
        long end = System.currentTimeMillis();
        System.out.println("Creating BPlusTree is end,the BPlus tree's height is " + bPlusTree.getHeight() + ", elapsed time is " + (end - start));
//        bPlusTree.printBPlusTree();

        //search by BPlus tree
        start = System.currentTimeMillis();
        Count result =  bPlusTree.get(17);
        end = System.currentTimeMillis();
        System.out.println("BPlus query elapsed time is " + (end - start) + " ms, result is " +result);

        //search by traverse
        start = System.currentTimeMillis();
        List<Count> resList = new ArrayList<>();
        for(Count count : countList){
            if(count.id == 17){
                resList.add(count);
            }
        }
        end = System.currentTimeMillis();
        System.out.println("Full query elapsed time is " + (end - start) + " ms, result size is " +resList.size());

    }

    public static BPlusTree<String,Count> createBPlusTreeBySensorName(List<Count> countList, int order){
        BPlusTree<String, Count> bPlusTree = new BPlusTree<>(order);
        countList.forEach(count -> bPlusTree.insertOrUpdate(count.sensorName,count));
        return bPlusTree;
    }

    public static BPlusTree<Integer,Count> createBPlusTreeById(List<Count> countList, int order){
        BPlusTree<Integer, Count> bPlusTree = new BPlusTree<>(order);
        countList.forEach(count -> bPlusTree.insertOrUpdate(count.id,count));
        return bPlusTree;
    }

    public static BPlusTree<Integer,Count> createBPlusTreeBySensorId(List<Count> countList, int order){
        BPlusTree<Integer, Count> bPlusTree = new BPlusTree<>(order);
        countList.forEach(count -> bPlusTree.insertOrUpdate(count.sensorId,count));
        return bPlusTree;
    }

    public static BPlusTree<Integer,Count> createBPlusTreeByTime(List<Count> countList, int order){
        BPlusTree<Integer, Count> bPlusTree = new BPlusTree<>(order);
        countList.forEach(count -> bPlusTree.insertOrUpdate(count.time,count));
        return bPlusTree;
    }
}