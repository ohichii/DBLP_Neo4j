
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class ConfigHandler1 extends DefaultHandler {

    HashMap<String, Element> entityMap = new HashMap<String, Element>();
    public int level = 0;
    boolean write = false;
    PrintWriter out;

    int elementCnt = 0;
    int pubCnt = 0;

    LinkedList<String> keys;

    @Override
    public void startDocument()
            throws SAXException {

        BufferedReader br = null;
        try {
            out = new PrintWriter("pub1.xml", "ISO-8859-1");
            out.println("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>");
            out.println("<!DOCTYPE dblp SYSTEM \"dblp.dtd\">");
            out.println("<dblp>");
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

        //new Neo4jCreator(entityMap);
        out.println("</dblp>");
        out.close();
        super.endDocument();
    }

    public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException {
        level++;

        if (level == 2) {
            elementCnt++;
            try {
                String key = attributes.getValue("key");
                //CHECK if this key in the Evaluation Dataset
                if (keys.contains(key) && pubCnt < 30352) {
                    pubCnt++;
                    write = true;
                    out.print("<" + qName);
                    for (int i = 0; i < attributes.getLength(); i++) {
                        String attName = attributes.getLocalName(i);
                        String attValue = attributes.getValue(i);
                        out.print(" " + attName + "=\"" + attValue + "\"");
                    }
                    out.println(">");
                    System.out.println("global : " + elementCnt);
                    System.out.println("writed : " + pubCnt);
                }
            } catch (Exception exc) {
                System.err.println("Wahd EXCEPTION KHAYBA!!");
            }
        } else if (level >= 3) {
            if (qName != null && write) {
                out.print("<" + qName + ">");
            }
        } 

    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        level--;
        if (write) {
            out.print("</" + qName + ">");
            if(level < 3){
                out.println();
            }
        }
        if (level == 1) {
            write = false;
        }
    }

    @Override
    public void characters(char ch[], int start, int length) throws SAXException {
        if (write) {
            out.print(new String(ch, start, length));
        }
    }

}
