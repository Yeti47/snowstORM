package net.yetibyte.snowstorm;

import java.util.Collection;

public interface IJoinedDatabaseObj extends IDatabaseReadable {
	
	Collection<Join> join();

}
