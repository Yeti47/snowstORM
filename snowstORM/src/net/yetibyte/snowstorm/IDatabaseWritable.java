package net.yetibyte.snowstorm;

/**
 * Interface f�r Objekte, welche in eine Datenbanktabelle geschrieben werden k�nnen.
 * @author Alexander Herrfurth
 *
 */
public interface IDatabaseWritable extends IDatabaseObj {
	
	/**
	 * Dient dem Schreiben dieses Datenbank-Objektes in die zugeh�rige Datenbanktabelle sowohl mittels INSERT INTO als auch UPDATE.
	 * Legt die Werte fest, mit welchen die Attribute bei der Ausf�hrung eines INSERT INTO oder UPDATE Befehls beschrieben werden sollen.
	 * Die Attribute k�nnen mittels einer Instanz der DatasetAttributes-Klasse den gew�nschten Werten zugewiesen werden. 
	 * Sollten Attributnamen verwendet werden, die in der Zieltabelle nicht existieren, so werden alle Schreibvorg�nge fehlschlagen.
	 * @return Eine Instanz von DatasetAttributes, welche die zu setzenden Attribute und deren Werte festlegt.
	 */
	DatasetAttributes writeToDatabase();

}
