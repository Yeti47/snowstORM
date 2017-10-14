package net.yetibyte.snowstorm;

/**
 * Interface für Objekte, welche in gesonderter Form serialisiert werden müssen, wenn sie als Wert eines Attributs in eine Datenbank
 * geschrieben werden. Standardmäßig wird als Attribut-Wert der von der toString-Methode zurückgegebene Wert verwendet. Wird jedoch eine
 * angepasste Abbildung des Objektes als String benötigt, kann dieses Interface implementiert werden.
 * @author Alexander Herrfurth
 *
 */
public interface IDatasetAttribute {
	
	/**
	 * Serialisiert dieses Objekt für das Schreiben in eine Datenbank als Attribut. 
	 * @return Eine Darstellung dieses Objektes als String.
	 */
	String attributeValue();

}
