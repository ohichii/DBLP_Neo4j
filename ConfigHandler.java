
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
//import java.util.Scanner;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author houhaich
 */
public class ConfigHandler extends DefaultHandler {

    HashMap<String, Element> entityMap = new HashMap<String, Element>();
    public int level = 0;
    String prevItemLevel2Key = "";
    boolean seenFirstAuthor = false;
    boolean seenAuthorStartElem = false;
    boolean seenEditorStartElem = false;
    boolean seenTitleStartElem = false;
    boolean seenCiteStartElem = false;
    boolean seenPagesStartElem = false;
    boolean seenYearStartElem = false;
    boolean seenVolumeStartElem = false;
    boolean seenJournalStartElem = false;
    boolean seenNumberStartElem = false;
    boolean seenUrlStartElem = false;
    boolean seenEeStartElem = false;
    boolean seenNoteStartElem = false;
    boolean seenCdRomStartElem = false;
    boolean seenCrossrefStartElem = false;
    boolean seenIsbnStartElem = false;
    boolean seenBooktitleStartElem = false;
    boolean seenSeriesStartElem = false;
    boolean seenPublisherStartElem = false;
    boolean seenMonthStartElem = false;
    boolean seenSchoolStartElem = false;
    boolean seenChapterStartElem = false;
    boolean seenAddressStartElem = false;
    boolean write = false;

    int elementCnt = 0;
    int pubCnt = 0;

    LinkedList<String> keys;

    @Override
    public void startDocument()
            throws SAXException {

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(new File("pubEvaluation.csv")));
            keys = new LinkedList<>();
            String availalbe;
            while ((availalbe = br.readLine()) != null) {
                String[] parts = availalbe.split(";");
                keys.add(parts[1]);
            }
            br.close();
        } catch (Exception ex) {
            Logger.getLogger(ConfigHandler1.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void endDocument() throws SAXException {

        new Neo4jCreator(entityMap);
        super.endDocument();
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
        // attributes.getValue("name")
        level++;
        elementCnt++;
        if ((elementCnt % 10000000) == 0) {
            System.out.println(elementCnt);
        }
        if (level == 2) {
            try {
                String key = attributes.getValue("key");
                // CHECK if this key in the Evaluation Dataset
                if (keys.contains(key) && pubCnt < 30352) {
                    write = true;
                    System.out.println(pubCnt++);
                    // check if this key already exists
                    seenFirstAuthor = false;

                    if (entityMap.get(key) != null) {
                        System.err.println("duplicate key! " + key);
                    }
                    Element entity = new Element();
                    entity.key = key;
                    prevItemLevel2Key = key;
                    entity.type = qName;
                    try {
                        entity.mdate = attributes.getValue("mdate");
                    } catch (Exception exc) {
                    }
                    entityMap.put(key, entity);
                    System.out.println(key);
                }
            } catch (Exception exc) {
                System.err.println("no key for item in level 2 is found!");
            }
        } else if (level == 3 && write) {
            if (null != qName) {
                switch (qName) {
                    case "author":
                        seenAuthorStartElem = true;
                        //seenFirstAuthor = true;
                        break;
                    case "editor":
                        seenEditorStartElem = true;
                        break;
                    case "title":
                        seenTitleStartElem = true;
                        break;
                    case "cite":
                        seenCiteStartElem = true;
                        break;
                    case "pages":
                        seenPagesStartElem = true;
                        break;
                    case "year":
                        seenYearStartElem = true;
                        break;
                    case "volume":
                        seenVolumeStartElem = true;
                        break;
                    case "journal":
                        seenJournalStartElem = true;
                        break;
                    case "number":
                        seenNumberStartElem = true;
                        break;
                    case "url":
                        seenUrlStartElem = true;
                        break;
                    case "ee":
                        seenEeStartElem = true;
                        break;
                    case "note":
                        seenNoteStartElem = true;
                        break;
                    case "cdRom":
                        seenCdRomStartElem = true;
                        break;
                    case "crossref":
                        seenCrossrefStartElem = true;
                        break;
                    case "isbn":
                        seenIsbnStartElem = true;
                        break;
                    case "booktitle":
                        seenBooktitleStartElem = true;
                        break;
                    case "series":
                        seenSeriesStartElem = true;
                        break;
                    case "publisher":
                        seenPublisherStartElem = true;
                        break;
                    case "month":
                        seenMonthStartElem = true;
                        break;
                    case "school":
                        seenSchoolStartElem = true;
                        break;
                    case "chapter":
                        seenChapterStartElem = true;
                        break;
                    case "address":
                        seenAddressStartElem = true;
                        break;
                    default:
                        break;
                }
            }
        }

    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        level--;
        if (level == 1) {
            write = false;
        }
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
        if (entityMap.get(prevItemLevel2Key) != null && write) {
            if (seenAuthorStartElem) {
                if (!seenFirstAuthor) {
                    entityMap.get(prevItemLevel2Key).authors.add(new String(ch, start, length) + " FirstOrSomething");
                    seenAuthorStartElem = false;
                    seenFirstAuthor = true;
                } else {
                    entityMap.get(prevItemLevel2Key).authors.add(new String(ch, start, length));
                    seenAuthorStartElem = false;
                }
            } else if (seenEditorStartElem) {
                entityMap.get(prevItemLevel2Key).editors.add(new String(ch, start, length));
                seenEditorStartElem = false;
            } else if (seenCiteStartElem) {
                entityMap.get(prevItemLevel2Key).cites.add(new String(ch, start, length));
                seenCiteStartElem = false;
            } else if (seenTitleStartElem) {
                entityMap.get(prevItemLevel2Key).title = new String(ch, start, length);
                seenTitleStartElem = false;
            } else if (seenCrossrefStartElem) {
                entityMap.get(prevItemLevel2Key).crossref = new String(ch, start, length);
                seenCrossrefStartElem = false;
            } else if (seenPagesStartElem) {
                entityMap.get(prevItemLevel2Key).pages = new String(ch, start, length);
                seenPagesStartElem = false;
            } else if (seenYearStartElem) {
                entityMap.get(prevItemLevel2Key).year = new String(ch, start, length);
                seenYearStartElem = false;
            } else if (seenAddressStartElem) {
                entityMap.get(prevItemLevel2Key).address = new String(ch, start, length);
                seenAddressStartElem = false;
            } else if (seenBooktitleStartElem) {
                entityMap.get(prevItemLevel2Key).booktitle = new String(ch, start, length);
                seenBooktitleStartElem = false;
            } else if (seenCdRomStartElem) {
                entityMap.get(prevItemLevel2Key).cdRom = new String(ch, start, length);
                seenCdRomStartElem = false;
            } else if (seenChapterStartElem) {
                entityMap.get(prevItemLevel2Key).chapter = new String(ch, start, length);
                seenChapterStartElem = false;
            } else if (seenEeStartElem) {
                entityMap.get(prevItemLevel2Key).ee = new String(ch, start, length);
                seenEeStartElem = false;
            } else if (seenMonthStartElem) {
                entityMap.get(prevItemLevel2Key).month = new String(ch, start, length);
                seenMonthStartElem = false;
            } else if (seenNumberStartElem) {
                entityMap.get(prevItemLevel2Key).number = new String(ch, start, length);
                seenNumberStartElem = false;
            } else if (seenNoteStartElem) {
                entityMap.get(prevItemLevel2Key).note = new String(ch, start, length);
                seenNoteStartElem = false;
            } else if (seenJournalStartElem) {
                entityMap.get(prevItemLevel2Key).journal = new String(ch, start, length);
                seenJournalStartElem = false;
            } else if (seenVolumeStartElem) {
                entityMap.get(prevItemLevel2Key).volume = new String(ch, start, length);
                seenVolumeStartElem = false;
            } else if (seenPublisherStartElem) {
                entityMap.get(prevItemLevel2Key).publisher = new String(ch, start, length);
                seenPublisherStartElem = false;
            } else if (seenIsbnStartElem) {
                entityMap.get(prevItemLevel2Key).isbn = new String(ch, start, length);
                seenIsbnStartElem = false;
            } else if (seenSeriesStartElem) {
                entityMap.get(prevItemLevel2Key).series = new String(ch, start, length);
                seenSeriesStartElem = false;
            } else if (seenUrlStartElem) {
                entityMap.get(prevItemLevel2Key).url = new String(ch, start, length);
                seenUrlStartElem = false;
            } else if (seenSchoolStartElem) {
                entityMap.get(prevItemLevel2Key).school = new String(ch, start, length);
                seenSchoolStartElem = false;
            }
        }
    }

}
