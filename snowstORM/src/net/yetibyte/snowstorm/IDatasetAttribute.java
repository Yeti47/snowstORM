package net.yetibyte.snowstorm;

/**
 * Interface f�r Objekte, welche in gesonderter Form serialisiert werden m�ssen, wenn sie als Wert eines Attributs in eine Datenbank
 * geschrieben werden. Standardm��ig wird als Attribut-Wert der von der toString-Methode zur�ckgegebene Wert verwendet. Wird jedoch eine
 * angepasste Abbildung des Objektes als String ben�tigt, kann dieses Interface implementiert werden.
 * @author Alexander Herrfurth
 *
 */
public interface IDatasetAttribute {
	
	/**
	 * Serialisiert dieses Objekt f�r das Schreiben in eine Datenbank als Attribut. 
	 * @return Eine Darstellung dieses Objektes als String.
	 */
	String attributeValue();

}
