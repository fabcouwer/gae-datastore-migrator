package net.styleguise.tools;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * This class assumes that persistent JPA classes are explicitly listed in the persistence.xml file.
 * If you are using JPA on Google App Engine and do not have your persistent classes explicitly listed
 * then you really need to do so because the classpath scanning technique is really slow and is undoubtedly
 * making your cold-start times longer than they need to be. If you have no idea what I'm talking about,
 * Google "JPA exclude-unlisted-classes".
 *
 * @author Benjamin Possolo
 */
public class PersistenceXmlReader {

	private static final String PersistenceXmlFile = "/META-INF/persistence.xml";

	private PersistenceXmlReader(){}

	public static List<Class<?>> readClasses(){
		try( InputStream persistenceXmlInputStream = PersistenceXmlReader.class.getResourceAsStream(PersistenceXmlFile) ){

			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			Document doc = docBuilder.parse(persistenceXmlInputStream);

			NodeList classNodes = doc.getElementsByTagName("class");
			ArrayList<Class<?>> classes = new ArrayList<>(classNodes.getLength());
			for( int i = 0; i < classNodes.getLength(); i++ ){
				Node classNode = classNodes.item(i);
				String className = classNode.getTextContent();
				classes.add(Class.forName(className));
			}
			return classes;
		}
		catch(Exception e){
			throw new RuntimeException(e);
		}
	}

}
