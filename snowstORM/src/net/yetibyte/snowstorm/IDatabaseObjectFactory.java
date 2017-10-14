package net.yetibyte.snowstorm;

/**
 * Funktionales Interface zur Instanziierung eines Datenbank-Objektes.
 * @author Alexander Herrfurth
 *
 * @param <T> Der Typ des Datenbank-Objektes.
 */
@FunctionalInterface
public interface IDatabaseObjectFactory<T extends IDatabaseReadable> {
	
	/**
	 * Erstellt eine Instanz des Datenbank-Objekts, welches dann nach dem Verbindungsaufbau mit einer Datenbank 
	 * weitergehend initialisiert werden kann.
	 * Hier können bereits datenbankunabhängige Initialisierungen vorgenommen werden.
	 * @return Die neu erstellte Instanz des Datenbank-Objekts.
	 */
	T createInstance();

}
