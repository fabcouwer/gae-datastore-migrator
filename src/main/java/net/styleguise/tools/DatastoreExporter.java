package net.styleguise.tools;

import java.io.BufferedWriter;
import java.io.Console;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entities;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.QueryResultList;

import static com.google.appengine.api.datastore.FetchOptions.Builder.*;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;

/**
 * Reads data from a Google App Engine datastore and writes it to comma
 * separated value files. Each kind is written to its own file and the files are
 * all written to a temporary directory. The directory and file names are
 * printed to standard out as they are written.
 *
 * Certain datastore kinds that represent metadata (like __Stat_Total__ and
 * _ah_SESSION) are not exported.
 *
 * @author Benjamin Possolo
 * Edited by Friso Abcouwer
 */
public class DatastoreExporter extends RemoteDatastoreClient {

	// ------------------------------------------------------------------------------------------------------
	// Class variables
	// ------------------------------------------------------------------------------------------------------

	public static final String FieldSeparator = ",";
	public static final char FieldSeparatorChar = ',';
	public static final String CollectionValueSeparator = ";";
	public static final char CollectionValueSeparatorChar = ';';
	public static final String DoubleQuote = "\"";
	public static final char DoubleQuoteChar = '"';
	public static final String DoubleDoubleQuote = "\"\"";
	public static final String CarriageReturn = "\r";
	public static final String EscapedCarriageReturn = "__R__";
	public static final String Newline = "\n";
	public static final String EscapedNewline = "__N__";
	public static final String Null = "NULL";
	public static final String Dash = "-";
	private static final String FileExtension = ".csv";
	private static final String DataDir = "gae-data-dump";
	private static final int PrefetchSize = 100;
	private static final int ChunkSize = 10;
	private static final String HttpSessionKind = "_ah_SESSION";
	private static final String StatKindCompositeIndex = "__Stat_Kind_CompositeIndex__";
	private static final String StatKindIsRootEntity = "__Stat_Kind_IsRootEntity__";
	private static final String StatKindNotRootEntity = "__Stat_Kind_NotRootEntity__";
	private static final String StatKind = "__Stat_Kind__";
	private static final String StatPropertyNameKind = "__Stat_PropertyName_Kind__";
	private static final String StatPropertyTypeKind = "__Stat_PropertyType_Kind__";
	private static final String StatPropertyTypePropertyNameKind = "__Stat_PropertyType_PropertyName_Kind__";
	private static final String StatPropertyType = "__Stat_PropertyType__";
	private static final String StatTotal = "__Stat_Total__";
	private static final String BlobFileIndex = "__BlobFileIndex__";
	private static final String BlobInfo = "__BlobInfo__";
	private static final String StatNamespace = "__Stat_Namespace__";

	// ------------------------------------------------------------------------------------------------------
	// Main method
	// ------------------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		Console console = System.console();

		System.out
				.println("Enter your GAE credentials for administering styleguise-marketplace");
		// String email = console.readLine("Email: ");'
		String email = args[0];
		// String password = new String(console.readPassword("Password: "));
		String password = args[1];

		System.out.println("Enter host and port for the remote datastore");
		// String host = console.readLine("Host (ex. APPNAME.appspot.com): ");
		String host = args[2];
		// int port =
		// Integer.parseInt(console.readLine("Port (on GAE 443, on localhost 8888): "));
		int port = 443;

		try (DatastoreExporter exporter = new DatastoreExporter(host, port,
				email, password)) {
			exporter.exportData();
		}
	}

	// ------------------------------------------------------------------------------------------------------
	// Constructors
	// ------------------------------------------------------------------------------------------------------

	public DatastoreExporter(String host, int port, String email,
			String password) throws IOException {
		super(host, port, email, password);
	}

	// ------------------------------------------------------------------------------------------------------
	// Public methods
	// ------------------------------------------------------------------------------------------------------

	public List<Path> exportData() throws IOException {

		// dir into which the CSV files will be written
		Path tmpDir = Files.createTempDirectory(DataDir);

		// fetch kinds in datastore
		List<String> kinds = getKinds();

		for (String s : kinds) {
			System.out.println("Kind: " + s);
		}

		// exclude kinds that have huge amounts of data or aren't necessary
		kinds.remove(HttpSessionKind);
		kinds.remove(StatKindCompositeIndex);
		kinds.remove(StatKindIsRootEntity);
		kinds.remove(StatKindNotRootEntity);
		kinds.remove(StatKind);
		kinds.remove(StatPropertyNameKind);
		kinds.remove(StatPropertyTypeKind);
		kinds.remove(StatPropertyTypePropertyNameKind);
		kinds.remove(StatPropertyType);
		kinds.remove(StatTotal);
		kinds.remove(BlobFileIndex);
		kinds.remove(BlobInfo);
		kinds.remove(StatNamespace);

		ArrayList<Path> dataFiles = new ArrayList<>(kinds.size());
		for (String kind : kinds) {
			System.out.println("Exporting " + kind);
			Path csv = Files.createTempFile(tmpDir, kind + Dash, FileExtension);
			try (PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter(csv.toFile())))) {
				writeKindDataSafely(writer, kind);
			}
			dataFiles.add(csv);
			System.out.println("Wrote " + csv);
		}
		System.out.println("Finished");
		return dataFiles;
	}

	// ------------------------------------------------------------------------------------------------------
	// Private methods
	// ------------------------------------------------------------------------------------------------------

	/**
	 * This method performs the query which fetches the entities from the
	 * datastore. The prefetch size dictates the number of results that are
	 * retrieved in the first batch. The chunk size dictates the number of
	 * results that are retrieved on subsequent remote reads.
	 *
	 * The entities are all added to a buffer and set of all property names is
	 * constructed. The set of property names is important so that we can build
	 * the CSV header line. The entities of the kind do not need to have
	 * homogeneous property names so an empty property value will appear as NULL
	 * in the CSV file.
	 *
	 * For very large datasets, this buffering technique will not work since the
	 * JVM may run out of memory. If you have large datasets, I recommend using
	 * a two-pass system (which will incur additional GAE costs): -the first
	 * pass should build the property name set and write the header line to the
	 * CSV file -the second pass should sequentially write the entities to the
	 * CSV file
	 */
	private void writeKindData(PrintWriter writer, String kind) {

		HashSet<String> propertyNames = new HashSet<>();
		ArrayList<Entity> entities = new ArrayList<>();

		FetchOptions options = FetchOptions.Builder.withPrefetchSize(
				PrefetchSize).chunkSize(ChunkSize);
		Query q = new Query(kind);

		for (Entity entity : ds.prepare(q).asIterable(options)) {
			entities.add(entity);
			propertyNames.addAll(entity.getProperties().keySet());
		}

		writeHeaderRow(writer, propertyNames);

		for (Entity entity : entities)
			writeEntity(writer, entity, propertyNames);
	}

	// Two-pass system
	// First pass: get the property names
	// Second pass: write entities
	private void writeKindDataSafely(PrintWriter writer, String kind) {

		HashSet<String> propertyNames = new HashSet<>();

		FetchOptions options = FetchOptions.Builder.withLimit(20);
		Query q = new Query(kind);

		// Retrieve property names from first result and write them
		QueryResultList<Entity> results = ds.prepare(q).asQueryResultList(
				options);
		for (Entity e : results) {
			propertyNames.addAll(e.getProperties().keySet());
		}
		writeHeaderRow(writer, propertyNames);

		// Continue while we are getting new results
		while (!results.isEmpty()) {
			// Write entities for this batch
			for (Entity e : results) {
				writeEntity(writer, e, propertyNames);
			}

			// Get the next batch
			options.startCursor(results.getCursor());
			results = ds.prepare(q).asQueryResultList(options);
		}
	}

	private void writeHeaderRow(PrintWriter writer,
			HashSet<String> propertyNames) {
		writer.write(Entity.KEY_RESERVED_PROPERTY);
		Iterator<String> i = propertyNames.iterator();
		if (i.hasNext())
			writer.write(FieldSeparator);
		while (i.hasNext()) {
			writer.write(i.next());
			if (i.hasNext())
				writer.write(FieldSeparator);
		}
		writer.println();
	}

	private void writeEntity(PrintWriter writer, Entity entity,
			HashSet<String> propertyNames) {
		writer.write(KeyFactory.keyToString(entity.getKey()));
		Iterator<String> i = propertyNames.iterator();
		if (i.hasNext())
			writer.write(FieldSeparator);
		while (i.hasNext()) {
			Object value = entity.getProperty(i.next());
			writeObject(writer, value);
			if (i.hasNext())
				writer.write(FieldSeparator);
		}
		writer.println();
	}

	/**
	 * Converts the Java object into a string and writes it to the CSV file. The
	 * following Java types are handled specially:
	 * <ul>
	 * <li>Java <code>null</code> is converted to the string "NULL"</li>
	 * <li>java.util.Date is converted to millisecond representation</li>
	 * <li>java.util.Collection (only java.util.Set and java.util.List are
	 * supported) are converted into a semi-colon separated string</li>
	 * <li>java.lang.Enum values are converted to a string by calling
	 * Enum.name()</li>
	 * <li>com.google.appengine.api.datastore.Key is converted to a websafe
	 * string using KeyFactory</li>
	 * <li>com.google.appengine.api.datastore.Text is escaped and written as a
	 * string</li>
	 * <li>any other type is converted to a string by calling Object.toString()</li>
	 * </ul>
	 */
	private void writeObject(PrintWriter writer, Object value) {

		if (value == null)
			writer.write(Null);

		else {
			if (value instanceof Key)
				writer.write(KeyFactory.keyToString((Key) value));

			else if (value instanceof Date)
				writer.write(Long.toString(((Date) value).getTime()));

			else if (value instanceof Collection)
				writeCollection(writer, (Collection<?>) value);

			else if (value instanceof Enum)
				writer.write(((Enum<?>) value).name());

			else if (value instanceof Text)
				escapeAndWrite(writer, ((Text) value).getValue());

			else
				escapeAndWrite(writer, value.toString());
		}

	}

	private void writeCollection(PrintWriter writer, Collection<?> collection) {
		Iterator<?> i = collection.iterator();
		while (i.hasNext()) {
			writeObject(writer, i.next());
			if (i.hasNext())
				writer.write(CollectionValueSeparator);
		}
	}

	/**
	 * Writes the string value to the CSV file. Converts " into "" Converts \n
	 * into __N__ Converts \r into __R__ Wraps a string that contains a comma
	 * with double quotes.
	 */
	private void escapeAndWrite(PrintWriter writer, String value) {
		if (value.contains(DoubleQuote)) {
			value = value.replaceAll(DoubleQuote, DoubleDoubleQuote);
		}
		if (value.contains(Newline)) {
			value = value.replaceAll(Newline, EscapedNewline);
		}
		if (value.contains(CarriageReturn)) {
			value = value.replaceAll(CarriageReturn, EscapedCarriageReturn);
		}
		if (value.contains(FieldSeparator)) {
			value = DoubleQuote + value + DoubleQuote;
		}
		writer.write(value);
	}

	/**
	 * Issues a query to fetch the datastore kinds.
	 */
	private List<String> getKinds() {
		ArrayList<String> kinds = new ArrayList<>();
		Query q = new Query(Entities.KIND_METADATA_KIND);
		FetchOptions options = FetchOptions.Builder
				.withPrefetchSize(PrefetchSize);
		for (Entity e : ds.prepare(q).asIterable(options))
			kinds.add(e.getKey().getName());
		return kinds;
	}
}
