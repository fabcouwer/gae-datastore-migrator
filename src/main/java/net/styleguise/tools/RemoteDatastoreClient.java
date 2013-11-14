package net.styleguise.tools;

import java.io.IOException;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;

public abstract class RemoteDatastoreClient implements AutoCloseable {

	private final RemoteApiInstaller installer = new RemoteApiInstaller();
	protected final DatastoreService ds;

	public RemoteDatastoreClient(String host, int port, String email, String password) throws IOException {
		RemoteApiOptions options = new RemoteApiOptions().server(host, port).credentials(email, password);
		installer.install(options);
		ds = DatastoreServiceFactory.getDatastoreService();
	}

	@Override
	public void close() throws Exception {
		installer.uninstall();
	}

}
