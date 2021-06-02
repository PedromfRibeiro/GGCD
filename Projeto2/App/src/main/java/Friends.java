import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Friends {

    public static void main(String[] args) {

        // spark configuration
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Friends");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // compute the collaborators of each actor, creating pairs (nconst, [nconst, ...])
        List<Tuple2<String,Set<String>>> friends = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.principals.tsv.bz2")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore non actors
                .filter(l -> l[3].contains("actor") || l[3].contains("actress"))
                // create pairs (tconst, nconst)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                // group by tconst, creating pairs (tconst, [nconst, ...])
                .groupByKey()
                // compute the collaborators of each actor, creating pairs (nconst, [nconst, ...])
                .mapToPair(p -> {
                    List<String> actors = Lists.newArrayList(p._2);
                    for (int i = 0; i < actors.size() - 1; i++) {
                        List<String> collaborators = actors;
                        collaborators.remove(i);
                        return new Tuple2<>(actors.get(i), collaborators);
                    }
                    // dummy pair
                    return new Tuple2<>("null", null);
                })
                // ignore dummy pairs
                .filter(p -> !p._1.equals("null"))
                .mapToPair(p -> new Tuple2<>(p._1, Lists.newArrayList(p._2)))
                // group by actor, creating pairs (nconst, [[const, ...], [nconst, ...]])
                .groupByKey()
                .mapToPair(p -> new Tuple2<>(p._1, Utils.flattenList(Lists.newArrayList(p._2))))
                // remove duplicated collaborators
                .mapToPair(p -> {
                    Set<String> collaborators = new HashSet<>(p._2);
                    return new Tuple2<>(p._1, collaborators);
                })
                // run the job
                .collect();

        // show results
        for (Tuple2<String, Set<String>> value : friends) {
            System.out.println(value._1 + ":");
            for (String collaborator : value._2) {
                System.out.println("  > " + collaborator);
            }
        }

        // close spark context
        sc.close();
    }
}
