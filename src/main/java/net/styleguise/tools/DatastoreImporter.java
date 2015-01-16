package net.styleguise.tools;

import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.persistence.OneToMany;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;

/**
 * Loads CSV data files into a GAE datastore. The datastore can be the Dev Server running in Eclipse
 * or it can be a remote datastore on the Google cloud.
 *
 * This class uses the persistent JPA classes in your project as a reference for determining how to
 * convert CSV string values into Java objects.
 *
 * It supports most of the modeling techniques that JPA + Datanucleus + GAE support including owned One-to-Many relationships.
 * The following datatypes are supported:
 * <ul>
 * 	<li>java.lang.String</li>
 * 	<li>java.lang.Long and long</li>
 * 	<li>java.lang.Integer and int</li>
 * 	<li>java.lang.Boolean and boolean</li>
 * 	<li>java.lang.Double and double</li>
 * 	<li>java.lang.Enum and your subclasses</li>
 * 	<li>java.util.Date</li>
 * 	<li>java.util.Set (must be parameterized field)</li>
 * 	<li>java.util.List (must be parameterized field)</li>
 * 	<li>com.google.appengine.api.datastore.Text</li>
 * 	<li>com.google.appengine.api.datastore.Key</li>
 * 	<li>null values</li>
 * </ul>
 *
 * @author Benjamin Possolo
 *
 */
public class DatastoreImporter extends RemoteDatastoreClient {

	//------------------------------------------------------------------------------------------------------
	//Class variables
	//------------------------------------------------------------------------------------------------------

	private static final int EntityBufferSize = 500;
	private static final String EMPTY = "";
	public static final String Localhost = "localhost";
	public static final int DevRemoteApiPort = 8888;

	//------------------------------------------------------------------------------------------------------
	//Main method
	//------------------------------------------------------------------------------------------------------

	/**
	 * Prompts the user for a directory and then loads the CSV files within that
	 * directory into the Dev datastore running on the localhost on port 8888.
	 */
	public static void main(String[] args) throws Exception {
		Console console = System.console();
		Path dataDir = Paths.get(console.readLine("CSV data dir: "));
		Path persistenceXmlFile = Paths.get(console.readLine("Path to JPA persistence XML file: "));
		List<Path> dataFiles = new ArrayList<>();
		try(
			DirectoryStream<Path> ds = Files.newDirectoryStream(dataDir);
			DatastoreImporter importer = new DatastoreImporter(
					Localhost, 
					DevRemoteApiPort, 
					persistenceXmlFile) ){
			addAll(dataFiles, ds.iterator());
			importer.importData(dataFiles);
		}
	}
	
	//------------------------------------------------------------------------------------------------------
	//Member variables
	//------------------------------------------------------------------------------------------------------
	
	private Path persistenceXmlFile;

	//------------------------------------------------------------------------------------------------------
	//Constructors
	//------------------------------------------------------------------------------------------------------

	public DatastoreImporter(String host, int port, Path persistenceXmlFile) throws IOException {
		// when connecting to dev datastore, the username and email may be empty strings
		super(host, port, EMPTY, EMPTY);
		this.persistenceXmlFile = persistenceXmlFile;
	}

	//------------------------------------------------------------------------------------------------------
	//Public methods
	//------------------------------------------------------------------------------------------------------

	public void importData(List<Path> csvFiles) throws IOException {

		List<Class<?>> persistentClasses = PersistenceXmlReader.readClasses(persistenceXmlFile);
		HashMap<String, Class<?>> kindToClassMap = new HashMap<>(persistentClasses.size());
		for( Class<?> clazz : persistentClasses )
			kindToClassMap.put(clazz.getSimpleName(), clazz);

		ArrayList<Entity> entityBuffer = new ArrayList<>(EntityBufferSize);

		for( Path csv : csvFiles ){
			String kind = parseFileNameForKind(csv);
			Class<?> persistentClass = kindToClassMap.get(kind);
			if( persistentClass == null ){
				System.out.println("Skipping " + kind + " because no respective Java class found");
				continue;
			}

			System.out.println("Loading " + csv);

			try( BufferedReader reader = new BufferedReader(new FileReader(csv.toFile())) ){

				String line = reader.readLine(); //read the header line
				String[] propertyNames = line.split(DatastoreExporter.FieldSeparator);

				while( (line = reader.readLine()) != null ){
					Entity e = readEntity(persistentClass, propertyNames, line);
					entityBuffer.add(e);
					if( entityBuffer.size() >= EntityBufferSize ){
						ds.put(entityBuffer);
						entityBuffer.clear();
					}
				}
			}
		}

		if( entityBuffer.size() > 0 )
			ds.put(entityBuffer);
		System.out.println("Finished");
	}

	//------------------------------------------------------------------------------------------------------
	//Package protected methods (for testing)
	//------------------------------------------------------------------------------------------------------

	String parseFileNameForKind(Path csv){
		String fileName = csv.getFileName().toString();
		return fileName.substring(0, fileName.indexOf(DatastoreExporter.Dash));
	}

	/**
	 * Converts a CSV row into a datastore Entity.
	 * @param javaClass the java class that is mapped to the datastore kind
	 * @param propertyNames defines the order of columns within the row
	 * @param row the comma separated values that make up the entity
	 */
	Entity readEntity(Class<?> javaClass, String[] propertyNames, String row){

		Entity entity = null;
		List<String> tokens = tokenize(row);

		int i = 0;
		for( String propertyName : propertyNames ){

			String token = tokens.get(i);

			if( Entity.KEY_RESERVED_PROPERTY.equals(propertyName) ){
				Key key = KeyFactory.stringToKey(token);
				entity = new Entity(key);
			}
			else if( DatastoreExporter.Null.equals(token) ){
				entity.setProperty(propertyName, null);
			}
			else{
				Field field = BeanUtil.getField(javaClass, propertyName);
				Class<?> fieldType = field.getType();
				Object propertyValue;
				if( isListOrSet(fieldType) ){
					Class<?> classOfObjectsInColletion;
					if( field.isAnnotationPresent(OneToMany.class) ){
						classOfObjectsInColletion = Key.class;
					}
					else{
						classOfObjectsInColletion = BeanUtil.getParameterizedTypeArguments(field).get(0);
					}
					propertyValue = readMultiValue(classOfObjectsInColletion, token);
				}
				else{
					propertyValue = readSingleValue(fieldType, token);
				}
				entity.setProperty(propertyName, propertyValue);
			}
			i++;
		}

		return entity;
	}

	/**
	 * Tokenizes a row of comma separated values. Tries to follow most of the rules here:
	 * http://en.wikipedia.org/wiki/Comma-separated_values
	 * but it does not support rows which have embedded carriage returns/new lines
	 * so the DatastoreExporter escapes \r and \n into __R__ and __N__ respectively.
	 * Static so it can be easily unit tested.
	 */
	static List<String> tokenize(String csvRow){

		ArrayList<String> tokens = new ArrayList<>();
		StringBuilder token = new StringBuilder();
		boolean inQuotedString = false;

		for( int i = 0; i < csvRow.length(); i++ ){

			char c = csvRow.charAt(i);
			switch(c){

			case DatastoreExporter.DoubleQuoteChar:
				if( inQuotedString ){
					if( nextCharIsDoubleQuote(i, csvRow) ){ //escaped double-quote
						token.append(c);
						i++; //"consume" the next double-quote
					}
					else{ //end of quoted string
						inQuotedString = false;
					}
				}
				else{
					inQuotedString = true;
				}
				break;

			case DatastoreExporter.FieldSeparatorChar:
				if( inQuotedString ){
					token.append(c);
				}
				else{
					tokens.add(token.toString());
					token.setLength(0);
				}
				break;

			default:
				token.append(c);
			}
		}

		tokens.add(token.toString());
		return tokens;
	}

	//------------------------------------------------------------------------------------------------------
	//Private methods
	//------------------------------------------------------------------------------------------------------

	/**
	 * Converts a string token into a Java object.
	 * @param type the type to which the token should be coerced
	 * @param token the property value
	 * @return java object
	 */
	private Object readSingleValue(Class<?> type, String token){

		if( DatastoreExporter.Null.equals(token) )
			return null;

		if( type.isEnum() )
			return token;

		if( type == String.class )
			return unescape(token);

		if( type == Date.class )
			return new Date(Long.parseLong(token));

		if( type == Long.class || type == Long.TYPE )
			return Long.parseLong(token);

		if( type == Boolean.class || type == Boolean.TYPE )
			return Boolean.valueOf(token);

		if( type == Integer.class || type == Integer.TYPE )
			return Integer.parseInt(token);

		if( type == Double.class || type == Double.TYPE )
			return Double.parseDouble(token);

		if( type == Text.class )
			return new Text(unescape(token));

		if( type == Key.class )
			return KeyFactory.stringToKey(token);

		return token;
	}

	private String unescape(String token){
		return token.replaceAll(DatastoreExporter.EscapedCarriageReturn, DatastoreExporter.CarriageReturn)
					.replaceAll(DatastoreExporter.EscapedNewline, DatastoreExporter.Newline);
	}

	private Collection<Object> readMultiValue(Class<?> classOfObjectsInCollection, String token){
		ArrayList<Object> list = new ArrayList<>();
		String[] values = token.split(DatastoreExporter.CollectionValueSeparator);
		for( String value : values )
			list.add(readSingleValue(classOfObjectsInCollection, value));
		return list;
	}

	private boolean isListOrSet(Class<?> type){
		return type == List.class || type == Set.class;
	}

	private static boolean nextCharIsDoubleQuote(int i, String csvRow){
		if( i + 1 >= csvRow.length() )
			return false;
		char c = csvRow.charAt(i+1);
		return c == DatastoreExporter.DoubleQuoteChar;
	}

	private static <T> void addAll(Collection<T> collection, Iterator<T> i){
		while( i.hasNext() )
			collection.add(i.next());
	}

}
