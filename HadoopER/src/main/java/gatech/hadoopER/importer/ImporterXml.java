/*
 * CS 4365 Project
 */

package gatech.hadoopER.importer;

import gatech.hadoopER.io.XMLInputFormat;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

/**
 *
 * @author eric
 */
public abstract class ImporterXml<F extends From, T extends To> extends Importer<F,T> {

    @Override
    protected void writableToFrom(Writable writable, F from) {
        Text value = (Text)writable;
        SAXBuilder sax = new SAXBuilder();
        HashMap<String,String> map = new HashMap<>();
        try {
            Document doc = sax.build(new StringReader(value.toString()));
            Element root = doc.getRootElement();
            for(Element child: root.getChildren()) {
                map.put(child.getName().toLowerCase(), child.getTextTrim());
            }
            from.readMap(map);
        } catch (JDOMException | IOException ex) {
            Logger.getLogger(ImporterXml.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    @Override
    protected Class<? extends InputFormat> getInputFormat() {
        return XMLInputFormat.class;
    }
    
}
