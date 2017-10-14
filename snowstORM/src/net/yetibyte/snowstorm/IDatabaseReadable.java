package net.yetibyte.snowstorm;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface für Objekte, welche durch das Auslesen einer Datenbanktabelle erzeugt werden können.
 * @author Alex
 *
 */
public interface IDatabaseReadable extends IDatabaseObj {
	
	/**
	 * Bestimmt die Namen der Tabellen-Spalten, welche für die Initialisierung des Objekts erforderlich sind.
	 * @return Die Namen der relevanten Tabellenfelder in einem Array.
	 */
	String[] getColumnNames();
	
	/**
	 * Dient der Initialisierung dieses Datenbank-Objektes. Mittels des übergebenen ResultSet-Objektes kann
	 * auf einen Datensatz aus der zugehörigen Tabelle und dessen Attribute zugegriffen werden, um die Eigenschaften dieses Objekts zu initialisieren.
	 * Handling von SQLExceptions ist bei der Implementierung nicht zwingend erforderlich, da diese bereits vom DatabaseAccessor beim Lesevorgang abgefangen werden.
	 * Um dies zu gewährleisten, muss der implementierten Methode lediglich eine entsprechende throws-Deklaration (SQLException) angefügt werden.
	 * @param rs Das ResultSet, welches Zugriff auf die zur Initialisierung dieses Objekts erforderlichen Tabellen-Attribute ermöglicht.
	 */
	void readFromDatabase(ResultSet rs) throws SQLException;

}
