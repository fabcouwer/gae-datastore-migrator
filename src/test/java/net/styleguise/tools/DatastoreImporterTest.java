package net.styleguise.tools;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class DatastoreImporterTest {

	@Test
	public void testTokenize(){

		String row = "one,two,three,\"four\",\"wild fox \"\"jumps\"\" over the bridge, sort of\",five";

		List<String> tokens = DatastoreImporter.tokenize(row);
		assertEquals("one", tokens.get(0));
		assertEquals("two", tokens.get(1));
		assertEquals("three", tokens.get(2));
		assertEquals("four", tokens.get(3));
		assertEquals("wild fox \"jumps\" over the bridge, sort of", tokens.get(4));
		assertEquals("five", tokens.get(5));
	}
}
