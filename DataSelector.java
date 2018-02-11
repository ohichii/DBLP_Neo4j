
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Stack;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
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
public class DataSelector {

    public static void selectPubEvaluation() throws FileNotFoundException, UnsupportedEncodingException, IOException {
        
        BufferedReader br = new BufferedReader(new FileReader(new File("pubEvaluation.csv")));
        LinkedList<String> keys = new LinkedList<>();
        String availalbe;
        while ((availalbe = br.readLine()) != null) {
            String[] parts = availalbe.split(";");
            keys.add(parts[1]);
        }
        
        String line;
        boolean write = false;
        String type = null;
        int count = 0;
        int lineCount = 0;
        PrintWriter out = new PrintWriter("pubDblpEva3.xml", "ISO-8859-1");
        
        BufferedReader brDblp = new BufferedReader(new FileReader(new File("dblp.xml")));
        out.println("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>");
        out.println("<!DOCTYPE dblp SYSTEM \"dblp.dtd\">");
        out.println("<dblp>");
        while ((line = brDblp.readLine()) != null) {

            if (line.contains("key=\"")) {
                String key = line.substring(line.indexOf("key=\"") + 5, line.indexOf("\">"));
                //System.out.println(key);
                if (keys.contains(key)) {
                    System.out.println(count++);
                    System.out.println(lineCount);
                    write = true;
                    //System.out.println(line);
                    type = line.substring(1, line.indexOf(" mdate"));
                }
            }
            if (write) {
                out.println(line);
                //System.out.println(line);
            }
            if (line.contains("</" + type)) {
                write = false;
            }
            lineCount++;
        }

        out.println("</dblp>");

        brDblp.close();
        out.close();
        br.close();

    }

    public static void fixFile(String fileName) throws FileNotFoundException, XMLStreamException {
        InputStream is = new FileInputStream(fileName);
        XMLInputFactory inputFactory = XMLInputFactory.newInstance();
        XMLEventReader eventReader = inputFactory.createXMLEventReader(is, "utf-8");
        Stack<StartElement> stack = new Stack<StartElement>();

        while (eventReader.hasNext()) {
            try {
                XMLEvent event = eventReader.nextEvent();
                if (event.isStartElement()) {
                    StartElement startElement = event.asStartElement();
                    System.out.println("processing element: " + startElement.getName().getLocalPart());
                    stack.push(startElement);
                }
                if (event.isEndElement()) {
                    stack.pop();
                }
            } catch (XMLStreamException e) {

                System.out.println("error in line: " + e.getLocation().getLineNumber());
                StartElement se = stack.pop();
                System.out.println("non-closed tag:" + se.getName().getLocalPart() + " " + se.getLocation().getLineNumber());

                throw e;
            }
        }
    }

    public static void main(String[] args) throws Exception, SAXException {
        //selectPubEvaluation();
        fixFile("out2.xml");
                
    }
}
