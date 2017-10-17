package net.yetibyte.snowstorm;

import java.util.*;
import javax.sql.DataSource;

import java.lang.reflect.Field;
import java.sql.*;

/**
 * Dient als Schnittstelle zwischen der Applikation und einer Datenbank. Kann u. a. Einträge aus Datenbanktabellen einholen und neue
 * Datensätze anlegen sowie aktualisieren.
 * @author Alexander Herrfurth
 *
 */
public class DatabaseAccessor {
	
	// Fields

	/**
	 * The DataSource that provides the Connection.
	 */
	private DataSource _dataSource = null;
	
	private boolean _allowUpdateWithoutWhere = true;
	private boolean _allowDeleteWithoutWhere = false;
	
	// Constructors
	
	/**
	 * Erzeugt einen neuen DatabaseAccessor, welcher die übergebene DataSource zum Verbindungsaufbau verwerndet.
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
	
	public void allowUpdateWithoutWhere(boolean flag) {
		_allowUpdateWithoutWhere = flag;
	}
	
	public boolean allowsUpdateWithoutWhere() {
		return _allowUpdateWithoutWhere;
	}
	
	public void allowDeleteWithoutWhere(boolean flag) {
		_allowDeleteWithoutWhere = flag;
	}
	
	public boolean allowsDeleteWithoutWhere() {
		return _allowDeleteWithoutWhere;
	}
	
	// Methods
	
	/**
	 * Ruft alle Datensätze in der Datenbanktabelle ab, welche mit der übergebenen Where-Klausel übereinstimmen und erzeugt 
	 * aus diesen Datenbank-Objekte des gewünschten Typs.
	 * @param <T> Der Typ der zu erzeugenden Datenbank-Objekte. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verfügung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die übergebenen SQL-Parameter ersetzt werden. Wird null übergeben, so wird keine Where-Klausel verwendet.
	 * @param sqlParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null übergeben, werden keine Parameter verwendet.
	 * @return Eine Sammlung von Datenbank-Objekten des gewünschten Typs.
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
			
			if(colNames == null)
				colNames = new String[0];
			
			StringBuilder colBuilder = new StringBuilder(colNames.length > 0 ? "" : "*");
			
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
	 * Erzeugt aus allen Datensätzen in der zu dem gewünschen Datenbank-Objekt gehörenden Datenbanktabelle ein Datenbank-Objekt und
	 * gibt die generierten Instanzen zurück.
	 * @param <T> Der Typ der zu erzeugenden Datenbank-Objekte. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verfügung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @param whereClause Die anzuwendende Where-Klausel. Wird null übergeben, so wird keine Where-Klausel verwendet.
	 * @return Eine Sammlung von Datenbank-Objekten des gewünschten Typs.
	 */
	public <T extends IDatabaseReadable> List<T> fetch(IDatabaseObjectFactory<T> objFactory, String whereClause) {
		
		return fetch(objFactory, whereClause, null);
		
	}
	
	/**
	 * Erzeugt aus allen Datensätzen in der zu dem gewünschen Datenbank-Objekt gehörenden Datenbanktabelle ein Datenbank-Objekt und
	 * gibt die generierten Instanzen zurück.
	 * @param <T> Der Typ der zu erzeugenden Datenbank-Objekte. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verfügung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @return Eine Sammlung von Datenbank-Objekten des gewünschten Typs.
	 */
	public <T extends IDatabaseReadable> List<T> fetch(IDatabaseObjectFactory<T> objFactory) {
		
		return fetch(objFactory, null, null);
		
	}
	
	public <T extends IDatabaseObj> List<T> autofetch(IDatabaseObjectFactory<T> objFactory, String whereClause, String[] sqlParams) {
		
		if(objFactory == null || _dataSource == null)
			return null;
		
		List<T> results = new ArrayList<T>();
		
		Connection connection = null;
		
		try {

			connection = _dataSource.getConnection();
		    
			T tempObj = objFactory.createInstance();
			
			Collection<Field> annotatedFields = ReflectionUtility.getFieldsWithAnnotation(tempObj.getClass(), TableAttribute.class);
			
			Map<String, Field> fieldMap = new HashMap<String, Field>();
			
			for(Field field : annotatedFields) {
				
				TableAttribute annotation = field.getAnnotation(TableAttribute.class);
				
				if(DatasetAttributes.isSafeAttributeName(annotation.column())) {
					
					fieldMap.put(annotation.column(), field);
					
				}
				
			}
			
			
			StringBuilder colBuilder = new StringBuilder();
			
			Set<String> attrNames = fieldMap.keySet();
			
			if(attrNames.isEmpty())
				return null;
			
			int currAttrIndex = 0;
			
			for(String attr : attrNames) {
				
				colBuilder.append(attr);
				
				if(currAttrIndex < attrNames.size()-1)
					colBuilder.append(", ");
				
				currAttrIndex++;
				
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
		    	
		    	for(String attrName : attrNames) {
		    		
		    		Field field = fieldMap.get(attrName);
		    		
		    		boolean wasAccessible = field.isAccessible();
    				field.setAccessible(true);
    				
    				if(IDatasetAttribute.class.isAssignableFrom(field.getType())) {
    					
    					IDatasetAttribute fieldValue = (IDatasetAttribute)field.getType().newInstance();
    					fieldValue.deserializeByAttributeValue(rs.getString(attrName));
    					
    				}
    				else
    					field.set(tempObj, rs.getObject(attrName));
    				
    				field.setAccessible(wasAccessible);
		    		
		    	}
		    	
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
	
	public <T extends IDatabaseObj> List<T> autofetch(IDatabaseObjectFactory<T> objFactory, String whereClause) {
		
		return autofetch(objFactory, whereClause, null);
		
	}
	
	public <T extends IDatabaseObj> List<T> autofetch(IDatabaseObjectFactory<T> objFactory) {
		
		return autofetch(objFactory, null, null);
		
	}
	
	/**
	 * Ruft den ersten Datensatz in der Datenbanktabelle ab, welcher mit der übergebenen Where-Klausel übereinstimmt und erzeugt 
	 * aus diesem ein Datenbank-Objekt des gewünschten Typs.
	 * @param <T> Der Typ des zu erzeugenden Datenbank-Objekts. Muss IDatabaseReadable implementieren.
	 * @param objFactory Ein IDatabaseObjectFactory-Objekt, welches eine Methode zur Erzeugung einer Instanz des Datenbank-Objektes zur Verfügung stellt. Kann als Lambda-Ausdruck angegeben werden.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die übergebenen SQL-Parameter ersetzt werden.
	 * @param sqlParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null übergeben, werden keine Parameter verwendet.
	 * @return Das erzeugte Datenbank-Objekt.
	 */
	public <T extends IDatabaseReadable> T fetchSingle(IDatabaseObjectFactory<T> objFactory, String whereClause, String[] sqlParams) {
		
		List<T> objs = fetch(objFactory, whereClause, sqlParams);
		
		return objs == null || objs.isEmpty() ? null : objs.get(0);
		
	}
	
	public <T extends IDatabaseObj> T autofetchSingle(IDatabaseObjectFactory<T> objFactory, String whereClause, String[] sqlParams) {
		
		List<T> objs = autofetch(objFactory, whereClause, sqlParams);
		
		return objs == null || objs.isEmpty() ? null : objs.get(0);
		
	}

	
	/**
	 * Schreibt das übergebene Datenbank-Objekt als neuen Datensatz in die zugehörige Datenbanktabelle.
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
			
		    PreparedStatement statement = prepareInsert(connection, dbObj.getTableName(), dbObj.writeToDatabase());
		    
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
	
	public boolean autoInsert(IDatabaseObj dbObj) {
		
		if(dbObj == null || _dataSource == null)
			return false;
		
		Connection connection = null;
		
		int rowsAffected = 0;
		
		try {
			
			connection = _dataSource.getConnection();
			
			DatasetAttributes dsAttributes = new DatasetAttributes();
						
			if(!dsAttributes.parseAnnotations(dbObj))
				return false;
			
		    PreparedStatement statement = prepareInsert(connection, dbObj.getTableName(), dsAttributes);
		    
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
	 * Aktualisiert den Datensatz, welcher dem übergebenen Datenbank-Objekt zugeordnet wird und mit der angegebenen Where-Klausel übereinstimmt.
	 * Es kann auf eine Where-Klausel verzichtet werden, indem für diese null übergeben wird. Es ist jedoch dringend zu beachten, dass dadurch ALLE
	 * in der zugehörigen Tabelle befindlchen Datensätze aktualisiert und somit überschrieben werden.
	 * @param dbObj Das Datenbank-Objekt, dessen Attribute in die zugehörige Datenbanktabelle geschrieben werden sollen.
	 * @param targetAttributes Eine Instanz von DatasetAttributes, welche die zu aktualisierenden Attribute festlegt. Wird null übergeben, wird die von dem Datenbank-Objekt mittels der Methode writeToDatabase
	 * zur Verfügung gestellte Instanz verwendet.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die übergebenen SQL-Parameter ersetzt werden.
	 * Wird null übergeben, so wird keine Where-Klausel verwendet.
	 * @param whereParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null übergeben, werden keine Parameter verwendet.
	 * @return Die Anzahl der von dem Update betroffenen Datensätze oder -1 im Falle eines Fehlers.
	 */
	public int update(IDatabaseWritable dbObj, DatasetAttributes targetAttributes, String whereClause, String[] whereParams) {
		
		if(dbObj == null || _dataSource == null || (!_allowUpdateWithoutWhere && whereClause == null))
			return -1;
		
		Connection connection = null;
		
		int rowsAffected = 0;
		
		try {
			
			connection = _dataSource.getConnection();
			
			DatasetAttributes dsAttributes = targetAttributes != null ? targetAttributes : dbObj.writeToDatabase();
			
		    PreparedStatement statement = prepareUpdate(connection, dbObj.getTableName(), dsAttributes, whereClause, whereParams);
		    
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
	 * Aktualisiert den Datensatz, welcher dem übergebenen Datenbank-Objekt zugeordnet wird und mit der angegebenen Where-Klausel übereinstimmt.
	 * Es kann auf eine Where-Klausel verzichtet werden, indem für diese null übergeben wird. Es ist jedoch dringend zu beachten, dass dadurch ALLE
	 * in der zugehörigen Tabelle befindlchen Datensätze aktualisiert und somit überschrieben werden.
	 * @param dbObj Das Datenbank-Objekt, dessen Attribute in die zugehörige Datenbanktabelle geschrieben werden sollen.
	 * @param whereClause Die anzuwendende Where-Klausel. Kann Platzhalter in Form eines ? enthalten, welche dann durch die übergebenen SQL-Parameter ersetzt werden.
	 * Wird null übergeben, so wird keine Where-Klausel verwendet.
	 * @param whereParams Ein Array mit Parametern, welche die in der Where-Klausel verwendeten Platzhalter ersetzen. Wird null übergeben, werden keine Parameter verwendet.
	 * @return Die Anzahl der von dem Update betroffenen Datensätze oder -1 im Falle eines Fehlers.
	 */
	public int update(IDatabaseWritable dbObj, String whereClause, String[] whereParams) {
		
		return update(dbObj, null, whereClause, whereParams);
		
	}
	
	public int autoupdate(IDatabaseObj dbObj, DatasetAttributes targetAttributes, String whereClause, String[] whereParams) {
		
		if(dbObj == null || _dataSource == null || (!_allowUpdateWithoutWhere && whereClause == null))
			return -1;
		
		Connection connection = null;
		
		int rowsAffected = 0;
		
		try {
			
			connection = _dataSource.getConnection();
			
			DatasetAttributes dsAttributes = targetAttributes;
			
			if(dsAttributes == null) {
				
				dsAttributes = new DatasetAttributes();
				
				if(!dsAttributes.parseAnnotations(dbObj))
					return -1;
				
			}
			
		    PreparedStatement statement = prepareUpdate(connection, dbObj.getTableName(), dsAttributes, whereClause, whereParams);
		    
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
	
	public int autoupdate(IDatabaseObj dbObj, String whereClause, String[] whereParams) {
		
		return autoupdate(dbObj, null, whereClause, whereParams);
		
	}
	
	public int autoupdate(IDatabaseObj dbObj, String whereClause) {
		
		return autoupdate(dbObj, null, whereClause, null);
		
	}
	
	public int autoupdateSubset(IDatabaseObj dbObj, Collection<String> attributeNames, String whereClause, String[] whereParams) {
		
		boolean updateAllowed = _allowUpdateWithoutWhere || whereClause != null;
		
		if(dbObj != null && updateAllowed) {
			
			DatasetAttributes dsAttributesOriginal = new DatasetAttributes();
			
			if(dsAttributesOriginal.parseAnnotations(dbObj)) {
				
				return autoupdate(dbObj, dsAttributesOriginal.createSubset(attributeNames), whereClause, whereParams);
				
			}
			
		}
		
		return -1;
		
	}
	
	public int autoupdateSubset(IDatabaseObj dbObj, String[] attributeNames, String whereClause, String[] whereParams) {
		
		return attributeNames != null ? autoupdateSubset(dbObj, Arrays.asList(attributeNames), whereClause, whereParams) : -1;
		
	}
	
	public int updateSubset(IDatabaseWritable dbObj, Collection<String> attributeNames, String whereClause, String[] whereParams) {
		
		boolean updateAllowed = _allowUpdateWithoutWhere || whereClause != null;
		
		if(dbObj != null && updateAllowed) {
			
			DatasetAttributes dsAttributesOriginal = dbObj.writeToDatabase();
			
			if(dsAttributesOriginal != null)
				return update(dbObj, dsAttributesOriginal.createSubset(attributeNames), whereClause, whereParams);
			
		}
		
		return -1;
		
	}
	
	public int updateSubset(IDatabaseWritable dbObj, String[] attributeNames, String whereClause, String[] whereParams) {
		
		boolean updateAllowed = _allowUpdateWithoutWhere || whereClause != null;
		
		if(dbObj != null && updateAllowed) {
			
			DatasetAttributes dsAttributesOriginal = dbObj.writeToDatabase();
			
			Collection<String> attributeNamesColl = attributeNames != null ? Arrays.asList(attributeNames) : new ArrayList<String>();
			
			if(dsAttributesOriginal != null)
				return update(dbObj, dsAttributesOriginal.createSubset(attributeNamesColl), whereClause, whereParams);
			
		}
		
		return -1;
		
	}
	
	public int delete(String tableName, String whereClause, String[] whereParams) {
		
		boolean allowDelete = _allowDeleteWithoutWhere || whereClause != null;
		
		if(!allowDelete || _dataSource == null || tableName == null)
			return -1;
		
		Connection connection = null;
		
		int rowsAffected = 0;
		
		try {
			
			connection = _dataSource.getConnection();
			
			String sql = "DELETE FROM " + tableName + (whereClause != null ? (" WHERE " + whereClause) : "");
			
		    PreparedStatement statement = connection.prepareStatement(sql);
		    
		    if(statement == null)
		    	return -1;
		    
		    if(whereClause != null && whereParams != null) {
				
				for(int j = 0; j < whereParams.length; j++)
					statement.setString(j+1, whereParams[j]);
				
			}
		    
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
	
	private PreparedStatement prepareInsert(Connection connection, String tableName, DatasetAttributes dsAttributes) throws SQLException {

		if(connection == null || tableName == null || dsAttributes == null)
			return null;
		
		//DatasetAttributes dsAttributes = dbObj.writeToDatabase();
		
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
		
		builder.append("INSERT INTO " + tableName + " (");
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
	
	private PreparedStatement prepareUpdate(Connection connection, String tableName, DatasetAttributes dsAttributes, String whereClause, String[] whereParams) throws SQLException {

		if(connection == null || tableName == null)
			return null;
		
		// DatasetAttributes dsAttributes = targetAttributes != null ? targetAttributes : dbObj.writeToDatabase();
		
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
		
		builder.append("UPDATE " + tableName + " ");
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
	
	public <T extends IJoinedDatabaseObj> List<T> fetchJoined(IDatabaseObjectFactory<T> objFactory, String whereClause, String[] sqlParams) {
		
		if(objFactory == null || _dataSource == null)
			return null;
		
		List<T> results = new ArrayList<T>();
		
		Connection connection = null;
		
		try {

			connection = _dataSource.getConnection();
		    
			T tempObj = objFactory.createInstance();
			
			String[] colNames = tempObj.getColumnNames();
			
			if(colNames == null)
				colNames = new String[0];
			
			StringBuilder colBuilder = new StringBuilder(colNames.length > 0 ? "" : "*");
			
			for(int i = 0; i < colNames.length; i++) {
				
				if(!DatasetAttributes.isSafeAttributeName(colNames[i]))
					return null;
				
				colBuilder.append(colNames[i]);
				
				if(i < colNames.length-1)
					colBuilder.append(", ");
				
			}
			
			String joinClause = " ";
			Collection<Join> joins = tempObj.join();
			
			if(joins == null)
				return null;
			
			for(Join join : joins) {
				
				if(!join.isValid())
					return null;
				
				joinClause += join.getClause() + " ";
				
			}
			
			String sql = "SELECT " + colBuilder.toString() + " FROM " + tempObj.getTableName() + joinClause + " " + (whereClause != null ? "WHERE " + whereClause : "");
			
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
	
	public <T extends IJoinedDatabaseObj> List<T> fetchJoined(IDatabaseObjectFactory<T> objFactory, String whereClause) {
		
		return fetchJoined(objFactory, whereClause, null);
		
	}
	
	public <T extends IJoinedDatabaseObj> List<T> fetchJoined(IDatabaseObjectFactory<T> objFactory) {
		
		return fetchJoined(objFactory, null, null);
		
	}
	
	public <T extends IJoinedDatabaseObj> T fetchJoinedSingle(IDatabaseObjectFactory<T> objFactory, String whereClause, String[] sqlParams) {
		
		List<T> objs = fetchJoined(objFactory, whereClause, sqlParams);
		
		return objs == null || objs.isEmpty() ? null : objs.get(0);
		
	}
	

}
