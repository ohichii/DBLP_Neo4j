
import javax.swing.ProgressMonitorInputStream;
import javax.xml.parsers.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;
import java.util.*;
import java.io.*;

public class Parser {

    public Set<String> author = new HashSet<String>();

    public static void main(String[] args) throws Exception, SAXException {

        System.setProperty("jdk.xml.entityExpansionLimit", "20000000");
        //System.setProperty("entityExpansionLimit", "20000000");
        try {

            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();

            DefaultHandler handler = new ConfigHandler();

            double sTime, eTime, duration;
            sTime = System.nanoTime();
            saxParser.parse("dblp.xml", handler);
            eTime = System.nanoTime();
            duration = (eTime - sTime) / 1e6;
            System.out.println("duration: " + duration);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}


