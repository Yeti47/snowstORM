package net.yetibyte.snowstorm;

import java.util.*;
import javax.sql.DataSource;
import java.sql.*;

/**
 * Dient als Schnittstelle zwischen der Applikation und einer Datenbank. Kann u. a. Eintr�ge aus Datenbanktabellen einholen und neue
 * Datens�tze anlegen sowie aktualisieren.
 * @author Alexander Herrfurth
 *
 */
public class DatabaseAccessor {
	
	// Fields

	/**
	 * The DataSource that provides the Connection.
	 */
	private DataSource _dataSource = null;
	
	// Constructors
	
	/**
	 * Erzeugt einen neuen DatabaseAccessor, welcher die �bergebene DataSource zum Verbindungsaufbau verwerndet.
	 * @param dataSource Die DataSource, die eine Datenbankverbindung bereitstellt.
	 */
	public DatabaseAccessor(DataSource dataSource) {
		
		_dataSource = dataSource;
		
	}
	
	// Getters / Setters
	
	public DataSource getDataSource() {
		return _dataSource;
	}
	
	public void setDataSource(DataSource dataSource) {
		_dataSource = dataSource;	
	}
	
	// Methods
	
	/**
	 * Ruft alle Datens�tze in der Datenbanktabelle ab, welche mit der �bergebenen Where-Klausel �bereinstimmen und erzeugt 
	 * aus diesen Datenbank-Objekte des gew�nschten Typs.
	 * @param <T> Der Typ der zu erzeugenden Datenbank-Objekte. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verf�gung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die �bergebenen SQL-Parameter ersetzt werden. Wird null �bergeben, so wird keine Where-Klausel verwendet.
	 * @param sqlParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null �bergeben, werden keine Parameter verwendet.
	 * @return Eine Sammlung von Datenbank-Objekten des gew�nschten Typs.
	 */
	public <T extends IDatabaseReadable> List<T> fetch(IDatabaseObjectFactory<T> objFactory, String whereClause, String[] sqlParams) {
		
		if(objFactory == null || _dataSource == null)
			return null;
		
		List<T> results = new ArrayList<T>();
		
		Connection connection = null;
		
		try {

			connection = _dataSource.getConnection();
		    
			T tempObj = objFactory.createInstance();
			
			String[] colNames = tempObj.getColumnNames();
			
			StringBuilder colBuilder = new StringBuilder(colNames == null || colNames.length > 0 ? "" : "*");
			
			for(int i = 0; i < colNames.length; i++) {
				
				if(!DatasetAttributes.isSafeAttributeName(colNames[i]))
					return null;
				
				colBuilder.append(colNames[i]);
				
				if(i < colNames.length-1)
					colBuilder.append(", ");
				
			}
			
			String sql = "SELECT " + colBuilder.toString() + " FROM " + tempObj.getTableName() + " " + (whereClause != null ? "WHERE " + whereClause : "");
			
		    PreparedStatement statement = connection.prepareStatement(sql);
		    
		    if(whereClause != null && sqlParams != null) {
		    	
		    	for(int i = 0; i < sqlParams.length; i++)
			    	statement.setString(i+1, sqlParams[i]);
		    	
		    }
		    
		    ResultSet rs = statement.executeQuery();
		    
		    while(rs.next()) {
		    	
		    	tempObj = objFactory.createInstance();
		    	tempObj.readFromDatabase(rs);
		    	results.add(tempObj);
		    	
		    }
		    
		}
	    catch(Exception e) {
	    	
	    	return null;
	    	
	    }
	    finally {
	    	
	    	if(connection != null) {
	    		
	    		try { connection.close(); }
	    		catch(Exception e) { }
	    		
	    	}
	    	
	    }
		
		return results;
		
	}
	
	/**
	 * Erzeugt aus allen Datens�tzen in der zu dem gew�nschen Datenbank-Objekt geh�renden Datenbanktabelle ein Datenbank-Objekt und
	 * gibt die generierten Instanzen zur�ck.
	 * @param <T> Der Typ der zu erzeugenden Datenbank-Objekte. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verf�gung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @param whereClause Die anzuwendende Where-Klausel. Wird null �bergeben, so wird keine Where-Klausel verwendet.
	 * @return Eine Sammlung von Datenbank-Objekten des gew�nschten Typs.
	 */
	public <T extends IDatabaseReadable> List<T> fetch(IDatabaseObjectFactory<T> objFactory, String whereClause) {
		
		return fetch(objFactory, whereClause, null);
		
	}
	
	/**
	 * Erzeugt aus allen Datens�tzen in der zu dem gew�nschen Datenbank-Objekt geh�renden Datenbanktabelle ein Datenbank-Objekt und
	 * gibt die generierten Instanzen zur�ck.
	 * @param <T> Der Typ der zu erzeugenden Datenbank-Objekte. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verf�gung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @return Eine Sammlung von Datenbank-Objekten des gew�nschten Typs.
	 */
	public <T extends IDatabaseReadable> List<T> fetch(IDatabaseObjectFactory<T> objFactory) {
		
		return fetch(objFactory, null, null);
		
	}
	
	/**
	 * Ruft den ersten Datensatz in der Datenbanktabelle ab, welcher mit der �bergebenen Where-Klausel �bereinstimmt und erzeugt 
	 * aus diesem ein Datenbank-Objekt des gew�nschten Typs.
	 * @param <T> Der Typ des zu erzeugenden Datenbank-Objekts. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verf�gung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die �bergebenen SQL-Parameter ersetzt werden.
	 * @param sqlParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null �bergeben, werden keine Parameter verwendet.
	 * @return Das erzeugte Datenbank-Objekt.
	 */
	public <T extends IDatabaseReadable> T fetchSingle(IDatabaseObjectFactory<T> objFactory, String whereClause, String[] sqlParams) {
		
		List<T> objs = fetch(objFactory, whereClause, sqlParams);
		
		return objs == null || objs.isEmpty() ? null : objs.get(0);
		
	}

	
	/**
	 * Schreibt das �bergebene Datenbank-Objekt als neuen Datensatz in die zugeh�rige Datenbanktabelle.
	 * @param dbObj Das zu schreibende Objekt.
	 * @return True bei Erfolg, false im Falle eines Fehlers.
	 */
	public boolean insert(IDatabaseWritable dbObj) {
		
		if(dbObj == null || _dataSource == null)
			return false;
		
		Connection connection = null;
		
		int rowsAffected = 0;
		
		try {
			
			connection = _dataSource.getConnection();
			
		    PreparedStatement statement = prepareInsert(connection, dbObj);
		    
		    if(statement == null)
		    	return false;
		    
		    rowsAffected = statement.executeUpdate();
		    
		}
	    catch(Exception e) {
	    	
	    	return false;
	    	
	    }
	    finally {
	    	
	    	if(connection != null) {
	    		
	    		try { connection.close(); }
	    		catch(Exception e) { }
	    		
	    	}
	    	
	    }
		
		return rowsAffected != 0;
		
	}
	
	/**
	 * Aktualisiert den Datensatz, welcher dem �bergebenen Datenbank-Objekt zugeordnet wird und mit der angegebenen Where-Klausel �bereinstimmt.
	 * Es kann auf eine Where-Klausel verzichtet werden, indem f�r diese null �bergeben wird. Es ist jedoch dringend zu beachten, dass dadurch ALLE
	 * in der zugeh�rigen Tabelle befindlchen Datens�tze aktualisiert und somit �berschrieben werden.
	 * @param dbObj Das Datenbank-Objekt, dessen Attribute in die zugeh�rige Datenbanktabelle geschrieben werden sollen.
	 * @param targetAttributes Eine Instanz von DatasetAttributes, welche die zu aktualisierenden Attribute festlegt. Wird null �bergeben, wird die von dem Datenbank-Objekt mittels der Methode writeToDatabase
	 * zur Verf�gung gestellte Instanz verwendet.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die �bergebenen SQL-Parameter ersetzt werden.
	 * Wird null �bergeben, so wird keine Where-Klausel verwendet.
	 * @param whereParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null �bergeben, werden keine Parameter verwendet.
	 * @return Die Anzahl der von dem Update betroffenen Datens�tze oder -1 im Falle eines Fehlers.
	 */
	public int update(IDatabaseWritable dbObj, DatasetAttributes targetAttributes, String whereClause, String[] whereParams) {
		
		if(dbObj == null || _dataSource == null)
			return -1;
		
		Connection connection = null;
		
		int rowsAffected = 0;
		
		try {
			
			connection = _dataSource.getConnection();
			
		    PreparedStatement statement = prepareUpdate(connection, dbObj, targetAttributes, whereClause, whereParams);
		    
		    if(statement == null)
		    	return -1;
		    
		    rowsAffected = statement.executeUpdate();
		    
		}
	    catch(Exception e) {
	    	
	    	return -1;
	    	
	    }
	    finally {
	    	
	    	if(connection != null) {
	    		
	    		try { connection.close(); }
	    		catch(Exception e) { }
	    		
	    	}
	    	
	    }
		
		return rowsAffected;
		
	}
	
	/**
	 * Aktualisiert den Datensatz, welcher dem �bergebenen Datenbank-Objekt zugeordnet wird und mit der angegebenen Where-Klausel �bereinstimmt.
	 * Es kann auf eine Where-Klausel verzichtet werden, indem f�r diese null �bergeben wird. Es ist jedoch dringend zu beachten, dass dadurch ALLE
	 * in der zugeh�rigen Tabelle befindlchen Datens�tze aktualisiert und somit �berschrieben werden.
	 * @param dbObj Das Datenbank-Objekt, dessen Attribute in die zugeh�rige Datenbanktabelle geschrieben werden sollen.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die �bergebenen SQL-Parameter ersetzt werden.
	 * Wird null �bergeben, so wird keine Where-Klausel verwendet.
	 * @param whereParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null �bergeben, werden keine Parameter verwendet.
	 * @return Die Anzahl der von dem Update betroffenen Datens�tze oder -1 im Falle eines Fehlers.
	 */
	public int update(IDatabaseWritable dbObj, String whereClause, String[] whereParams) {
		
		return update(dbObj, null, whereClause, whereParams);
		
	}
	
	private PreparedStatement prepareInsert(Connection connection, IDatabaseWritable dbObj) throws SQLException {

		if(connection == null || dbObj == null)
			return null;
		
		DatasetAttributes dsAttributes = dbObj.writeToDatabase();
		
		if(dsAttributes == null || dsAttributes.hasInvalidAttributeName())
			return null;
						
		int attrCount = dsAttributes.count();
		
		if(attrCount <= 0)
			return null;
		
		int attrIndex = 0;
		
		Collection<String> attrNames = dsAttributes.getAttributeNames();
		
		List<String> attrValues = new ArrayList<String>();
		
		StringBuilder columnBuilder = new StringBuilder();
		StringBuilder valueBuilder = new StringBuilder();
		
		for(String attrName : attrNames) {
			
			columnBuilder.append(attrName);
			
			if(attrIndex < attrCount - 1)
				columnBuilder.append(", ");
			
			valueBuilder.append("?");
			
			if(attrIndex < attrCount - 1)
				valueBuilder.append(", ");
			
			attrValues.add(dsAttributes.readAttribute(attrName));
			
			attrIndex++;
		}
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("INSERT INTO " + dbObj.getTableName() + " (");
		builder.append(columnBuilder);
		builder.append(") VALUES (");
		builder.append(valueBuilder);
		builder.append(')');
		
		String sql = builder.toString();
		
		PreparedStatement statement = connection.prepareStatement(sql);

		for(int i = 0; i < attrValues.size(); i++)
			statement.setString(i+1, attrValues.get(i));
				
		return statement;
		
	}
	
	private PreparedStatement prepareUpdate(Connection connection, IDatabaseWritable dbObj, DatasetAttributes targetAttributes, String whereClause, String[] whereParams) throws SQLException {

		if(connection == null || dbObj == null)
			return null;
		
		DatasetAttributes dsAttributes = targetAttributes != null ? targetAttributes : dbObj.writeToDatabase();
		
		if(dsAttributes == null || dsAttributes.hasInvalidAttributeName())
			return null;
						
		int attrCount = dsAttributes.count();
		
		if(attrCount <= 0)
			return null;
		
		int attrIndex = 0;
		
		Collection<String> attrNames = dsAttributes.getAttributeNames();
		
		List<String> attrValues = new ArrayList<String>();
		
		StringBuilder setBuilder = new StringBuilder();
		setBuilder.append("SET ");
		
		for(String attrName : attrNames) {
			
			setBuilder.append(attrName + "= ?");
			
			if(attrIndex < attrCount - 1)
				setBuilder.append(", ");
			
			attrValues.add(dsAttributes.readAttribute(attrName));
			
			attrIndex++;
		}
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("UPDATE " + dbObj.getTableName() + " ");
		builder.append(setBuilder);

		if(whereClause != null) {
			
			builder.append(" WHERE " + whereClause);
						
		}
		
		String sql = builder.toString();
		
		PreparedStatement statement = connection.prepareStatement(sql);
		
		for(int i = 0; i < attrCount; i++)
			statement.setString(i+1, attrValues.get(i));
		
		if(whereClause != null && whereParams != null) {
			
			for(int j = 0; j < whereParams.length; j++)
				statement.setString(j+1+attrCount, whereParams[j]);
			
		}
				
		return statement;
		
	}
	

}
