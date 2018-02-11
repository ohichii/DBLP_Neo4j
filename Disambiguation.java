/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
//import org.neo4j.kernel.impl.util.StringLogger;

/**
 *
 * @author azmah
 */
public class Disambiguation {

    /**
     * @param args the command line arguments
     */
    public static void groupPublications() throws FileNotFoundException, IOException {

        Scanner inUniq = new Scanner(new File("uniq.csv"));

        while (inUniq.hasNext()) {
            Scanner inPub = new Scanner(new File("pubTest.csv"));
            String next = inUniq.nextLine();
            next = next.trim();
            //String fileName = next.replaceAll("\\s+","");
            File f = new File("groups2/" + next + ".csv");
            FileWriter out = new FileWriter(f);
            //System.out.println(next);
            while (inPub.hasNext()) {
                String next1 = inPub.nextLine();

                if (next1.contains(next)) {
                    //System.out.println(next1);
                    out.write(next1 + "\n");
                }
            }
            out.close();
            inPub.close();
        }
        inUniq.close();
    }

    public enum NodeType implements Label {
        publication, CoAuthor;
    }

    public enum RelationType implements RelationshipType {
        coauthored;
    }

    public static void createIndexes(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {

            db.execute("CREATE INDEX ON :Publication(Author);");
            db.execute("CREATE CONSTRAINT ON (p:Publication) ASSERT p.PId IS UNIQUE;");
            db.execute("CREATE CONSTRAINT ON (c:CoAuthor) ASSERT c.Name IS UNIQUE;");

            tx.success();
        }
    }

    public static void createNodes(GraphDatabaseService graphDb) throws FileNotFoundException {
        Node result = null;
        ResourceIterator<Node> resultIterator = null;

        try (Transaction tx = graphDb.beginTx()) {

            long startTime = System.currentTimeMillis();
            File dir = new File("groups");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                System.out.println("MAKING THE GRAPH !!");
                for (File child : directoryListing) {

                    String fileName = child.getName();
                    int index = fileName.lastIndexOf('.');
                    if (index > 0) {
                        fileName = fileName.substring(0, index);
                    }
                    //ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
                    Scanner in = new Scanner(child);
                    while (in.hasNext()) {
                        String next = in.nextLine();
                        //System.out.println(next);
                        String[] line = next.split(";");
                        //System.out.println(line);
                        String[] authors = line[2].split(",");
                        //System.out.println(line[0] + " ** " + line[1] + " ** " + authors[0]);
                        //Node pub = graphDb.createNode(NodeType.Publication);
                        String createPublication = "MERGE (p:Publication {PId: {PId}}) RETURN p";
                        Map<String, Object> params = new HashMap<>();
                        params.put("PId", line[0]);
                        resultIterator = graphDb.execute(createPublication, params).columnAs("p");
                        Node pub = resultIterator.next();
                        pub.setProperty("key", line[1]);
                        pub.setProperty("Author", authors[0]);
                        System.out.println(pub.getProperty("PId"));
                        for (int i = 1; i < authors.length; i++) {
                            String author = authors[i];
                            String queryString = "MERGE (n:CoAuthor {Name: {Name}}) RETURN n";
                            Map<String, Object> parameters = new HashMap<>();
                            parameters.put("Name", authors[0] + "_" + author);
                            //parameters.put("Name", author);
                            resultIterator = graphDb.execute(queryString, parameters).columnAs("n");
                            result = resultIterator.next();
                            result.createRelationshipTo(pub, RelationType.coauthored);
                        }
                    }
                    in.close();

                }
                System.out.println("GRAPH MADE !!");
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                System.out.println(totalTime);
            }
            tx.success();
        }
    }

    public static void createNodes2(GraphDatabaseService graphDb) throws FileNotFoundException, IOException {
        Node result = null;
        ResourceIterator<Node> resultIterator = null;

        try (Transaction tx = graphDb.beginTx()) {

            File pubListFile = new File("pubEvaluation.csv");

            //ExecutionEngine engine = new ExecutionEngine(graphDb, StringLogger.SYSTEM);
            //Scanner in = new Scanner(pubListFile);
            BufferedReader brIn = new BufferedReader(new FileReader(pubListFile));
            int j = 0;
            String next;
            while ((next = brIn.readLine()) != null) {
                //String next = in.nextLine();
                //System.out.println(next);
                String[] line = next.split(";");
                String[] authors = line[2].split(",");
                Node pub = graphDb.createNode(NodeType.publication);
                ++j;
                pub.setProperty("PId", line[0]);
                pub.setProperty("key", line[1]);
                String authorName = "";

                for (int i = 0; i < authors.length; i++) {
                    String a = authors[i];
                    //System.out.println("a: "+a);
                    if (a.matches(".*\\d+.*")) {
                        authorName = a;
                        authorName = authorName.replaceAll("\\s+$", ""); // Remove spaces at the end
                        authorName = authorName.replaceFirst("^\\s*", "");// Remove spaces at the begining
                        //System.out.println("authorName: " + authorName);
                        break;
                    }
                }
                pub.setProperty("dis_author", authorName);
                authorName = authorName.replaceAll("\\d", ""); // Remove digits
                authorName = authorName.replaceAll("\\s+$", ""); // Remove spaces at the end
                authorName = authorName.replaceFirst("^\\s*", "");// Remove spaces at the begining
                pub.setProperty("author", authorName);
                for (int i = 0; i < authors.length; i++) {
                    if (!authors[i].matches(".*\\d+.*")) {
                        String coauthor = authors[i];
                        coauthor = coauthor.replaceAll("\\d", ""); // Remove digits
                        coauthor = coauthor.replaceAll("\\s+$", ""); // Remove spaces at the end
                        coauthor = coauthor.replaceFirst("^\\s*", "");// Remove spaces at the begining
                        String queryString = "MERGE (n:CoAuthor {Name: {Name}}) RETURN n";
                        Map<String, Object> parameters = new HashMap<>();
                        parameters.put("Name", coauthor);
                        resultIterator = graphDb.execute(queryString, parameters).columnAs("n");
                        result = resultIterator.next();
                        result.createRelationshipTo(pub, RelationType.coauthored);
                    }

                }
            }
            System.out.println("*******************  " + j + "  ************************");
            brIn.close();

            //disambiguate(graphDb);
            tx.success();
        }
    }

    public static void disambiguate(GraphDatabaseService db) throws FileNotFoundException, IOException {
        int authorIdCounter = 0;
        try (Transaction tx = db.beginTx()) {

            long start = System.currentTimeMillis();
            System.out.println("DISAMBIGUATION !!");
            String line = null;
            //BufferedReader br = new BufferedReader(new FileReader(new File("outUniq2.csv")));
            //while ((line = br.readLine()) != null) {
            //    String nextName = line;
            ResourceIterator<Node> nodes = db.findNodes(Disambiguation.NodeType.publication/*, "author", nextName*/);

            while (nodes.hasNext()) {
                Node n1 = nodes.next();

                if (!n1.hasProperty("authorId")) {
                    n1.setProperty("authorId", authorIdCounter);
                }
                int authorId = (int) n1.getProperty("authorId");
                //System.out.println("N11111 -->  " + n1.getProperty("author") + " --> " + n1.getProperty("authorId"));

                ResourceIterator<Node> nodes2 = db.findNodes(Disambiguation.NodeType.publication, "author", n1.getProperty("author"));
                while (nodes2.hasNext()) {

                    Node n2 = nodes2.next();
                    String queryString = "OPTIONAL MATCH (n:publication { PId:{PId1} }),(m:publication { PId:{PId2}}), p = shortestPath((n)-[*..6]-(m))\n"
                            + "RETURN length(p)";
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("PId1", n1.getProperty("PId"));
                    parameters.put("PId2", n2.getProperty("PId"));

                    //System.out.println("N222222 -->  " + n2.getProperty("author"));
                    Result result = db.execute(queryString, parameters);
                    if (result.hasNext()) {
                        Integer distance = (Integer) result.next().get("length(p)");
                        //System.out.println(distance);
                        if (distance != null && distance < 5) {
                            //System.out.println("YES threshold");
                            if (n2.hasProperty("authorId")) {
                                //System.out.println("YES authorId");
                                Integer i = (Integer) n2.getProperty("authorId");
                                if (i != authorId) {
                                    ResourceIterator<Node> nodesToEdit = db.findNodes(Disambiguation.NodeType.publication, "authorId", i);
                                    while (nodesToEdit.hasNext()) {
                                        Node next = nodesToEdit.next();
                                        next.setProperty("authorId", authorId);
                                        //System.out.println("EDIT -->  " + next.getProperty("author") + " --> " + next.getProperty("authorId"));
                                    }
                                }
                            } else {
                                n2.setProperty("authorId", authorId);
                            }
                        }
                    }
                }
                nodes2.close();
                System.out.println(++authorIdCounter);

            }
            //}
            // br.close();
            System.out.println("DISAMBIGUATION DONE !!");
            long end = System.currentTimeMillis();
            System.out.println(end - start);

            tx.success();
        }

    }

    public static void disambiguate2(GraphDatabaseService db, int threshold) throws FileNotFoundException {
        int authorId = 0;
        try (Transaction tx = db.beginTx()) {

            long start = System.currentTimeMillis();
            System.out.println("DISAMBIGUATION !!");

            ResourceIterator<Node> nodes = db.findNodes(Label.label("publication"));

            while (nodes.hasNext()) {
                Node n1 = nodes.next();
                //System.out.println("fffffffffffffffffffffffff");
                if (n1.hasProperty("firstAuthor") && !n1.hasProperty("authorId")) {
                    n1.setProperty("authorId", authorId);
                    // Find publications that have the same author name.
                    ResourceIterator<Node> nodes2 = db.findNodes(Label.label("publication"), "firstAuthor", n1.getProperty("firstAuthor"));
                    while (nodes2.hasNext()) {

                        Node n2 = nodes2.next();
                        if (n1.getId() != n2.getId()) {
                            String query1 = "OPTIONAL MATCH (n:publication { PId:{PId1} }),(m:publication { PId:{PId2}}),"
                                    + " p = shortestPath((n)-[r:WRITTEN_BY *..6]-(m))\n"
                                    + "RETURN length(p)";
                            String query2 = "OPTIONAL MATCH (n:publication { PId:{PId1} }),(m:publication { PId:{PId2}}),"
                                    + " p = shortestPath((n)-[r:IN_JOURNAL *..3]-(m))\n"
                                    + "RETURN length(p)";
                            String query3 = "OPTIONAL MATCH (n:publication { PId:{PId1} }),(m:publication { PId:{PId2}}),"
                                    + " p = shortestPath((n)-[r:IN_YEAR *..3]-(m))\n"
                                    + "RETURN length(p)";
                            Map<String, Object> parameters = new HashMap<>();
                            parameters.put("PId1", n1.getProperty("PId"));
                            parameters.put("PId2", n2.getProperty("PId"));

                            //System.out.println("N222222 -->  " + n2.getProperty("firstAuthor"));
                            Result result1 = db.execute(query1, parameters);
                            Result result2 = db.execute(query2, parameters);
                            Result result3 = db.execute(query3, parameters);
                            if (result1.hasNext()) {
                                Long distance1 = (Long) result1.next().get("length(p)");
                                //System.out.println(distance);
                                if (distance1 != null && distance1 < threshold) {
                                    System.out.println("Distance 1");
                                    if (n2.hasProperty("authorId")) {
                                        //System.out.println("YES has authorId");
                                        Integer i = (Integer) n2.getProperty("authorId");
                                        if (i != authorId) {
                                            //System.out.println("YES diffrent");
                                            ResourceIterator<Node> nodesToEdit = db.findNodes(Label.label("publication"), "authorId", i);
                                            while (nodesToEdit.hasNext()) {
                                                Node next = nodesToEdit.next();
                                                next.setProperty("authorId", authorId);
                                                System.out.println("EDIT -->  " + next.getProperty("firstAuthor") + " --> " + next.getProperty("authorId"));
                                            }
                                        }
                                    } else {
                                        n2.setProperty("authorId", authorId);
                                    }
                                } else if (result2.hasNext() && result3.hasNext()) {
                                    Long distance2 = (Long) result2.next().get("length(p)");
                                    Long distance3 = (Long) result3.next().get("length(p)");
                                    if (distance2 != null && distance3 != null) {
                                        if (distance2 < 3 && distance3 < 3) {
                                            System.out.println("Distance 2 & 3");
                                            if (n2.hasProperty("authorId")) {
                                                //System.out.println("YES has authorId");
                                                Integer i = (Integer) n2.getProperty("authorId");
                                                if (i != authorId) {
                                                    //System.out.println("YES diffrent");
                                                    ResourceIterator<Node> nodesToEdit = db.findNodes(Label.label("publication"), "authorId", i);
                                                    while (nodesToEdit.hasNext()) {
                                                        Node next = nodesToEdit.next();
                                                        next.setProperty("authorId", authorId);
                                                        System.out.println("EDIT -->  " + next.getProperty("firstAuthor") + " --> " + next.getProperty("authorId"));
                                                    }
                                                }
                                            } else {
                                                n2.setProperty("authorId", authorId);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    nodes2.close();
                    ++authorId;
                }
            }

            System.out.println("DISAMBIGUATION DONE !!");
            long end = System.currentTimeMillis();
            System.out.println(end - start);

            tx.success();
        }

    }

    public static void computeAccuracy(GraphDatabaseService db) throws FileNotFoundException {
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodes = db.findNodes(Disambiguation.NodeType.publication);
            int count = 0;
            while (nodes.hasNext()) {
                Node next = nodes.next();
                if (next.hasProperty("authorId")) {
                    //System.out.println(next.getProperty("dis_author"));
                    // Compute precision
                    String queryClusterSize = "MATCH (n {authorId: {authorId}}) RETURN  count(n)";
                    Map<String, Object> parameters = new HashMap<>();
                    parameters.put("authorId", next.getProperty("authorId"));
                    Long s1 = (Long) db.execute(queryClusterSize, parameters).next().get("count(n)");
                    double clusterSize = s1.doubleValue();
                    String queryDisNbre = "MATCH (n {authorId: {authorId}, dis_author: {dis_author}}) RETURN  count(n)";
                    parameters.put("dis_author", next.getProperty("dis_author"));
                    Long s2 = (Long) db.execute(queryDisNbre, parameters).next().get("count(n)");
                    double disNbre = s2.doubleValue();
                    Double P = disNbre / clusterSize;
                    next.setProperty("BcubedPrecision", P);
                    //System.out.println(P);

                    // Compute recall
                    String queryGroupAuhtorSize = "MATCH (n {dis_author: {dis_author}}) RETURN  count(n)";
                    //parameters.put("dis_author", next.getProperty("dis_author"));
                    Long s3 = (Long) db.execute(queryGroupAuhtorSize, parameters).next().get("count(n)");
                    double groupAuthorSize = s3.doubleValue();
                    Double R = disNbre / groupAuthorSize;
                    next.setProperty("BcubedRecall", R);
                    //System.out.println(P);

                    // Compute F
                    Double alfa = 0.5;
                    Double F = 1 / (alfa * (1 / P) + (1 - alfa) * (1 / R));
                    next.setProperty("F_measure", F);
                    //System.out.println(F);
                    System.out.println(count++);
                }
            }
            tx.success();
        }
    }

    public static void checkResults(GraphDatabaseService db) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        //Scanner in = new Scanner(new File("testPublicationsAndEvaluation.csv"));
        BufferedReader br = new BufferedReader(new FileReader(new File("testPublicationsAndEvaluation.csv")));
        String availalbe;
        PrintWriter out = new PrintWriter("recallResults2.csv", "UTF-8");
        int count = 0;
        try (Transaction tx = db.beginTx()) {
            while ((availalbe = br.readLine()) != null) {
                String next = availalbe;
                String[] parts = next.split(";");
                ResourceIterator<Node> result = db.findNodes(Label.label("publication"), "PId", parts[0]);
                if (result.hasNext()) {
                    count++;
                    Node node = result.next();
                    if (node.hasProperty("BcubedRecall")) {
                        out.println(parts[0] + ";" + parts[7] + ";" + node.getProperty("BcubedRecall"));
                        System.out.println(parts[0] + ";" + parts[7] + ";" + node.getProperty("BcubedRecall"));
                    } else {
                        out.println(parts[0] + ";" + parts[7] + ";" + "None");
                        System.out.println(parts[0] + ";" + parts[7] + ";" + "None");
                    }
                } else {
                    out.println(parts[0] + ";" + parts[7] + ";" + "None");
                    System.out.println(parts[0] + ";" + parts[7] + ";" + "None");
                }
            }
            System.out.println(count);
            out.close();
            br.close();
        }
    }

    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        //groupPublications();

        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        File dbFile = new File("/home/azmah/Desktop/thesis_IRIT/neo4j-community-3.1.4/data/databases/graph.db");
        GraphDatabaseService graphDb = dbFactory.newEmbeddedDatabase(dbFile);

        //createIndexes(graphDb);
        //createNodes2(graphDb);
        //graphDb.execute("MATCH (n) REMOVE n.authorId RETURN count(n)");
        disambiguate2(graphDb, 5);
        //computeAccuracy(graphDb);
        //checkResults(graphDb);
        //selectPubEvaluation();
        //graphDb.shutdown();
    }

}
