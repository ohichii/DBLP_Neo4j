
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

public class Neo4jCreator {

    HashMap<String, Long> distinctAuthors;
    HashMap<String, Long> distinctEditors;
    HashMap<String, Element> elementMap;
    HashMap<String, Long> elementKeyNodeMap;
    //private BatchInserter db;
    //private BatchInserterIndex index;
    GraphDatabaseFactory dbFactory;
    GraphDatabaseService db;
    private static final String KEY_PROPERTY = "key";
    private int totalElement = 0;
    HashSet<Long> nodeIdCreated = new HashSet<Long>();
    BatchInserterIndexProvider indexProvider;

    private static enum RelTypes implements RelationshipType {
        FIRST_AUTHOR, EDITED_BY, WRITTEN_BY, CITE_TO, IS_CROSSREF_WITH, PUBLISHED_BY, IN_ADDRESS, IN_YEAR, IN_SCHOOL, IN_JOURNAL
    }

    public Neo4jCreator(HashMap<String, Element> elementMap) {
        Map<String, String> config = new HashMap<String, String>();
        config.put("dbms.pagecache.memory", "50000M");
        config.put("dbms.pagecache.pagesize", "8g");
        config.put("node_auto_indexing", "true");
        //db = BatchInserters.inserter("dblp2.db", config);
        //indexProvider = new LuceneBatchInserterIndexProvider(db);
        dbFactory = new GraphDatabaseFactory();
        File dbFile = new File("/home/azmah/Desktop/thesis_IRIT/neo4j-community-3.1.4/data/databases/graph.db");

        db = dbFactory.newEmbeddedDatabase(dbFile);

        db.execute("CREATE CONSTRAINT ON (y:Year) ASSERT y.value IS UNIQUE;");
        db.execute("CREATE CONSTRAINT ON (j:Journal) ASSERT j.name IS UNIQUE;");
        db.execute("CREATE CONSTRAINT ON (a:author) ASSERT a.fullName IS UNIQUE;");
        db.execute("CREATE CONSTRAINT ON (e:editor) ASSERT e.name IS UNIQUE;");
        db.execute("CREATE CONSTRAINT ON (s:school) ASSERT s.name IS UNIQUE;");

        //index = indexProvider.nodeIndex("dblpIndex", MapUtil.stringMap("type", "exact"));
        //index.setCacheCapacity(KEY_PROPERTY, 500000001);
        distinctAuthors = new HashMap<String, Long>();
        distinctEditors = new HashMap<String, Long>();
        elementKeyNodeMap = new HashMap<String, Long>();
        this.elementMap = elementMap;
        for (String key : elementMap.keySet()) {
            Element element = elementMap.get(key);
            for (String author : element.authors) {
                if (distinctAuthors.get(author) == null && !author.contains("FirstOrSomething")) {
                    distinctAuthors.put(author, 0L);
                }
            }
            for (String editor : element.editors) {
                if (distinctEditors.get(editor) == null) {
                    distinctEditors.put(editor, 0L);
                }
            }
        }
        System.gc();
        System.out.println("create the graph db is called!");
        createTheGraphDb();
    }

    private void createTheGraphDb() {
        System.out.println("There are this number of distinct authors: " + distinctAuthors.keySet().size());
        System.out.println("There are this number of distinct editors: " + distinctEditors.keySet().size());
        System.out.println("There are this number of entities: " + elementMap.keySet().size());
        // tx = db.beginTx();
        try  {
            Transaction tx = db.beginTx();
            for (String author : distinctAuthors.keySet()) {
                totalElement++;

                Label label = DynamicLabel.label("author");
                Node node = db.createNode(label);
                node.setProperty("fullName", author);
                String[] parts = author.split(" ");
                if (parts.length == 1) {
                    if (parts[0].replaceAll("\\.", "").length() == 1) {
                        node.setProperty("oneLetterName", parts[0]);
                    } else if (parts[0].replaceAll("\\.", "").length() > 1) {
                        node.setProperty("completeLastName", parts[0]);
                    }
                } else if (parts.length >= 2) {
                    if (parts[0].replaceAll("\\.", "").length() == 1) {
                        node.setProperty("firstNameInitial", parts[0].charAt(0));
                    } else if (parts[0].replaceAll("\\.", "").length() > 1) {
                        node.setProperty("firstNameInitial", parts[0].charAt(0));
                        node.setProperty("completeFirstName", parts[0]);
                    }
                    for (int i = 1; i < parts.length - 1; i++) {
                        String part = parts[i];
                        if (part.replaceAll("\\.", "").length() == 1) {
                            node.setProperty("middleNameInitial" + i, part.charAt(0));
                        }
                        if (part.replaceAll("\\.", "").length() > 1) {
                            node.setProperty("middleNameInitial" + i, part.charAt(0));
                            node.setProperty("completeMiddleName" + i, part);
                        }
                    }
                    if (parts[parts.length - 1].replaceAll("\\.", "").length() == 1) {
                        node.setProperty("lastNameInitial", parts[parts.length - 1].charAt(0));
                    }
                    if (parts[parts.length - 1].replaceAll("\\.", "").length() > 1) {
                        node.setProperty("lastNameInitial", parts[parts.length - 1].charAt(0));
                        node.setProperty("completeLastName", parts[parts.length - 1]);
                    }
                }

                long nodeId = node.getId();

                distinctAuthors.put(author, nodeId);
                nodeIdCreated.add(nodeId);
                if ((totalElement % 1000) == 0) {
                    //index.flush();
                    tx.success();
                    System.out.println("authors: " + totalElement);
                }
            }

            System.out.println("distinctAuthors map nodes created!");

            for (String editor : distinctEditors.keySet()) {
                totalElement++;
                Label label = DynamicLabel.label("editor");
                Node node = db.createNode(label);
                node.setProperty("name", editor);
                String[] parts = editor.split(" ");
                if (parts.length > 0) {
                    String initials = String.valueOf(parts[0].charAt(0)) + ". ";
                    initials += parts[parts.length - 1];
                    node.setProperty("initials", initials);
                }
                Long nodeId = node.getId();
                nodeIdCreated.add(nodeId);
                distinctEditors.put(editor, nodeId);
                if ((totalElement % 1000) == 0) {
                    //index.flush();
                    tx.success();
                    System.out.println("editors: " + totalElement);
                }
            }

            System.out.println("distinctEditors map nodes created!");

            long pId = 0;
            // we can MERGE authors directly here to distinguish between the first author and the coathors;
            for (String key : elementMap.keySet()) {
                totalElement++;

                if ((totalElement % 1000000) == 0) {
                    System.out.println("current number of entities: " + totalElement);
                }
                Element element = elementMap.get(key);

                ArrayList<Label> labels = new ArrayList<Label>();
                labels.add(DynamicLabel.label("publication"));
                if (element.type != null) {
                    labels.add(DynamicLabel.label(element.type));
                }

                if (labels.size() > 0) {
                    Node node = db.createNode(labels.toArray(new Label[labels.size()]));
                    Long nodeId = node.getId();
                    elementKeyNodeMap.put(key, nodeId);
                    nodeIdCreated.add(nodeId);

                    node.setProperty("PId", pId++);

                    if (element.authors != null && element.authors.size() > 0) {
                        for (String author : element.authors) {
                            if (author.contains("FirstOrSomething")) {
                                author = author.replaceAll(" FirstOrSomething", "");
                                if (author.matches(".*\\d+.*")) {
                                    node.setProperty("disambiguated_author", author);
                                    author = author.replaceAll("\\d", ""); // Remove digits
                                    author = author.replaceAll("\\s+$", ""); // Remove spaces at the end
                                    author = author.replaceFirst("^\\s*", "");// Remove spaces at the begining
                                }
                                node.setProperty("firstAuthor", author);
                                String createPublication = "MERGE (a:author {fullName: {fullName}}) RETURN a";
                                Map<String, Object> params = new HashMap<>();
                                params.put("fullName", author);
                                ResourceIterator<Node> resultIterator = db.execute(createPublication, params).columnAs("a");
                                Node n = resultIterator.next();
                                node.createRelationshipTo(n, RelTypes.FIRST_AUTHOR);
                                String[] parts = author.split(" ");
                                if (parts.length == 1) {
                                    if (parts[0].replaceAll("\\.", "").length() == 1) {
                                        n.setProperty("oneLetterName", parts[0]);
                                    } else if (parts[0].replaceAll("\\.", "").length() > 1) {
                                        n.setProperty("completeLastName", parts[0]);
                                    }
                                } else if (parts.length >= 2) {
                                    if (parts[0].replaceAll("\\.", "").length() == 1) {
                                        n.setProperty("firstNameInitial", parts[0].charAt(0));
                                    } else if (parts[0].replaceAll("\\.", "").length() > 1) {
                                        n.setProperty("firstNameInitial", parts[0].charAt(0));
                                        n.setProperty("completeFirstName", parts[0]);
                                    }
                                    for (int i = 1; i < parts.length - 1; i++) {
                                        String part = parts[i];
                                        if (part.replaceAll("\\.", "").length() == 1) {
                                            n.setProperty("middleNameInitial" + i, part.charAt(0));
                                        }
                                        if (part.replaceAll("\\.", "").length() > 1) {
                                            n.setProperty("middleNameInitial" + i, part.charAt(0));
                                            n.setProperty("completeMiddleName" + i, part);
                                        }
                                    }
                                    if (parts[parts.length - 1].replaceAll("\\.", "").length() == 1) {
                                        n.setProperty("lastNameInitial", parts[parts.length - 1].charAt(0));
                                    }
                                    if (parts[parts.length - 1].replaceAll("\\.", "").length() > 1) {
                                        n.setProperty("lastNameInitial", parts[parts.length - 1].charAt(0));
                                        n.setProperty("completeLastName", parts[parts.length - 1]);
                                    }
                                }
                            }
                        }
                    }
                    if (element.address != null) {
                        node.setProperty("address", element.address);
                        String createPublication = "MERGE (a:Address {value: {value}}) RETURN a";
                        Map<String, Object> params = new HashMap<>();
                        params.put("value", element.address);
                        ResourceIterator<Node> resultIterator = db.execute(createPublication, params).columnAs("a");
                        Node n = resultIterator.next();
                        node.createRelationshipTo(n, RelTypes.IN_ADDRESS);
                    }
                    if (element.title != null) {
                        node.setProperty("title", element.title);
                    }
                    if (element.booktitle != null) {
                        node.setProperty("booktitle", element.booktitle);
                    }
                    if (element.cdRom != null) {
                        node.setProperty("cdRom", element.cdRom);
                    }
                    if (element.chapter != null) {
                        node.setProperty("chapter", element.chapter);
                    }
                    if (element.ee != null) {
                        node.setProperty("ee", element.ee);
                    }
                    if (element.crossref != null) {
                        node.setProperty("crossref", element.crossref);
                    }
                    if (element.isbn != null) {
                        node.setProperty("isbn", element.isbn);
                    }
                    if (element.journal != null) {
                        node.setProperty("journal", element.journal);
                        String createPublication = "MERGE (j:Journal {name: {name}}) RETURN j";
                        Map<String, Object> params = new HashMap<>();
                        params.put("name", element.journal);
                        ResourceIterator<Node> resultIterator = db.execute(createPublication, params).columnAs("j");
                        Node n = resultIterator.next();
                        node.createRelationshipTo(n, RelTypes.IN_JOURNAL);
                    }
                    if (element.key != null) {
                        node.setProperty("key", element.key);
                    }
                    if (element.mdate != null) {
                        node.setProperty("mdate", element.mdate);
                    }
                    if (element.month != null) {
                        node.setProperty("month", element.month);
                    }
                    if (element.note != null) {
                        node.setProperty("note", element.note);
                    }
                    if (element.number != null) {
                        node.setProperty("number", element.number);
                    }
                    if (element.pages != null) {
                        node.setProperty("pages", element.pages);
                    }
                    if (element.publisher != null) {
                        node.setProperty("publisher", element.publisher);
                    }
                    if (element.series != null) {
                        node.setProperty("series", element.series);
                    }
                    if (element.school != null) {
                        node.setProperty("school", element.school);
                        String createPublication = "MERGE (s:School {name: {name}}) RETURN s";
                        Map<String, Object> params = new HashMap<>();
                        params.put("name", element.school);
                        ResourceIterator<Node> resultIterator = db.execute(createPublication, params).columnAs("s");
                        Node n = resultIterator.next();
                        node.createRelationshipTo(n, RelTypes.IN_SCHOOL);
                    }
                    if (element.type != null) {
                        node.setProperty("type", element.type);
                    }
                    if (element.url != null) {
                        node.setProperty("url", element.url);
                    }
                    if (element.volume != null) {
                        node.setProperty("volume", element.volume);
                    }
                    if (element.year != null) {
                        node.setProperty("year", element.year);
                        String createPublication = "MERGE (y:Year {value: {value}}) RETURN y";
                        Map<String, Object> params = new HashMap<>();
                        params.put("value", element.year);
                        ResourceIterator<Node> resultIterator = db.execute(createPublication, params).columnAs("y");
                        Node n = resultIterator.next();
                        node.createRelationshipTo(n, RelTypes.IN_YEAR);
                    }
                }

                if ((totalElement % 1000) == 0) {
                    System.out.println("entities: " + totalElement);
                    //index.flush();
                    tx.success();
                }
            }
            System.out.println("all nodes added!");

            long totalElement1 = 0;
            for (String key : elementMap.keySet()) {

                Element element = elementMap.get(key);
                for (String author : element.authors) {
                    //db.createRelationship(elementKeyNodeMap.get(key), distinctAuthors.get(author), RelTypes.WRITTEN_BY,null);
                    if (!author.contains("FirstOrSomething")) {
                        db.getNodeById(elementKeyNodeMap.get(key)).createRelationshipTo(db.getNodeById(distinctAuthors.get(author)), RelTypes.WRITTEN_BY);
                    }
                }
                for (String editor : element.editors) {
                    //db.createRelationship(elementKeyNodeMap.get(key), distinctAuthors.get(editor), RelTypes.EDITED_BY, null);
                    db.getNodeById(elementKeyNodeMap.get(key)).createRelationshipTo(db.getNodeById(distinctEditors.get(editor)), RelTypes.EDITED_BY);
                }
                for (String cite : element.cites) {
                    if (nodeIdCreated.contains(elementKeyNodeMap.get(cite))) {
                        //db.createRelationship(elementKeyNodeMap.get(key), elementKeyNodeMap.get(cite), RelTypes.CITE_TO,null);
                        db.getNodeById(elementKeyNodeMap.get(key)).createRelationshipTo(db.getNodeById(elementKeyNodeMap.get(cite)), RelTypes.CITE_TO);
                    }
                }

                if (elementKeyNodeMap.get(element.crossref) != null
                        && nodeIdCreated.contains(elementKeyNodeMap.get(element.crossref))) {
                    //db.createRelationship(elementKeyNodeMap.get(key), elementKeyNodeMap.get(element.crossref), RelTypes.IS_CROSSREF_WITH, null);
                    db.getNodeById(elementKeyNodeMap.get(key)).createRelationshipTo(db.getNodeById(elementKeyNodeMap.get(element.crossref)), RelTypes.IS_CROSSREF_WITH);
                }
                if (elementKeyNodeMap.get(element.publisher) != null
                        && nodeIdCreated.contains(elementKeyNodeMap.get(element.publisher))) {
                    //db.createRelationship(elementKeyNodeMap.get(key), elementKeyNodeMap.get(element.publisher), RelTypes.PUBLISHED_IN, null);
                    db.getNodeById(elementKeyNodeMap.get(key)).createRelationshipTo(db.getNodeById(elementKeyNodeMap.get(element.publisher)), RelTypes.PUBLISHED_BY);
                }
                if ((totalElement1++ % 10000) == 0) {
                    //index.flush();
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                    System.out.println("relationship: " + totalElement1);
                }
            }

            tx.success();
        } catch (Exception e){
            
        }
        System.out.println("all relationship added!");

        System.out.println("indexProvider shutting down");
        //indexProvider.shutdown();

        System.out.println("db shutting down");
        db.shutdown();

        System.out.println("program is finished!");

    }
}
