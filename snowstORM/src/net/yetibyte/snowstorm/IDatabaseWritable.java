package net.yetibyte.snowstorm;

/**
 * Interface für Objekte, welche in eine Datenbanktabelle geschrieben werden können.
 * @author Alexander Herrfurth
 *
 */
public interface IDatabaseWritable extends IDatabaseObj {
	
	/**
	 * Dient dem Schreiben dieses Datenbank-Objektes in die zugehörige Datenbanktabelle sowohl mittels INSERT INTO als auch UPDATE.
	 * Legt die Werte fest, mit welchen die Attribute bei der Ausführung eines INSERT INTO oder UPDATE Befehls beschrieben werden sollen.
	 * Die Attribute können mittels einer Instanz der DatasetAttributes-Klasse den gewünschten Werten zugewiesen werden. 
	 * Sollten Attributnamen verwendet werden, die in der Zieltabelle nicht existieren, so werden alle Schreibvorgänge fehlschlagen.
	 * @return Eine Instanz von DatasetAttributes, welche die zu setzenden Attribute und deren Werte festlegt.
	 */
	DatasetAttributes writeToDatabase();

}
