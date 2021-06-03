import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public class Utils {

    // comparator for Tuple2<String, Integer>, descending order
    public static class MyComparatorID implements Comparator<Tuple2<String, Integer>> {
        @Override
        public int compare(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            return - Integer.compare(value1._2, value2._2);
        }
    }

    // comparator for Tuple2<String, Integer>, ascending order
    public static class MyComparatorIA implements Serializable, Comparator<Tuple2<String, Integer>> {
        @Override
        public int compare(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            return Integer.compare(value1._2, value2._2);
        }
    }

    // comparator for Tuple2<String, Double>, descending order
    public static class MyComparatorD implements Comparator<Tuple2<String, Double>> {
        @Override
        public int compare(Tuple2<String, Double> value1, Tuple2<String, Double> value2) {
            return - Double.compare(value1._2, value2._2);
        }
    }

    // flatten an ArrayList of ArrayLists to an ArrayList
    public static ArrayList<String> flattenList(ArrayList<ArrayList<String>> nestedList) {
        ArrayList<String> list = new ArrayList<>();
        nestedList.forEach(list::addAll);
        return list;
    }

    // count the occurences of each String in a List
    public static Iterator<Tuple2<String, Integer>> countOccurrences(List<String> list) {
        Set<String> distinct = new HashSet<>(list);
        List<Tuple2<String, Integer>> tuples = new ArrayList<>();
        for (String s: distinct) {
            tuples.add(new Tuple2<>(s, Collections.frequency(list, s)));
        }
        Collections.sort(tuples, new MyComparatorID());
        return tuples.iterator();
    }
}
