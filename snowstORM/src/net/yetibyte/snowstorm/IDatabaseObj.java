package net.yetibyte.snowstorm;


/**
 * Interface f�r Objekte, welche in einer Datenbanktabelle abgebildet werden k�nnen.
 * Ein Datenbank-Objekt beschreibt einen Datensatz einer bestimmten Tabelle.
 * @author Alexander Herrfurth
 *
 */
public interface IDatabaseObj {
	
	/**
	 * Bestimmt den Namen der Tabelle, in welcher die Attribute des Objekts enthalten sind.
	 * @return Der Name der Tabelle.
	 */
	String getTableName();

}
