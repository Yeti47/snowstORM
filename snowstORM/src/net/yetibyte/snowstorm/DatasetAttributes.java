package net.yetibyte.snowstorm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Beschreibt die Attribute eines Datenbank-Objekts. Diese Klasse ist dafür zuständig, den Attributen (bzw. Felder oder Spalten)
 * einer Datenbanktabelle beliebige Werte zuzuordnen. Dies ist für das Schreiben eines Objektes, welches IDatabaseObj implementiert, erforderlich.
 * @author Alexander Herrfurth.
 *
 */
public class DatasetAttributes {
	
	// Constants
	
	private final static char[] UNSAFE_ATTR_NAME_CHARS = { '=', '/', '\\', '\'', '"', ';' ,'´', '`', ',', '*' };  
	
	// Fields
	
	private Map<String, Object> _attributeMap;
	
	// Constructors
	
	public DatasetAttributes() {
		
		_attributeMap = new HashMap<String, Object>();
		
	}
	
	public DatasetAttributes(Map<String, Object> map) {
		
		_attributeMap = new HashMap<String, Object>(map);
		
	}
	
	// Methods
	
	public Set<String> getAttributeNames() {
		
		return _attributeMap.keySet();
		
	}
	
	public Collection<Object> getAttributeValues() {
		
		return _attributeMap.values();
		
	}
	
	public Collection<String> readAttributes() {
		
		ArrayList<String> results = new ArrayList<String>();
		
		for(Object o : _attributeMap.values())
			results.add(o.toString());
				
		return results;
		
	}
	
	public Object setAttribute(String name, Object obj) {
		
		return _attributeMap.put(name, obj);
		
	}
	
	public Object getAttribute(String name) {
		
		return _attributeMap.get(name);
		
	}
	
	public String readAttribute(String name) {
		
		Object attr = _attributeMap.get(name);
		
		if(attr == null)
			return null;
		
		if(attr instanceof IDatasetAttribute) {
			
			IDatasetAttribute dsAttr = (IDatasetAttribute)attr;
			return dsAttr.attributeValue();
			
		}
		
		return attr.toString();
		
	}
	
	public int count() {
		
		return _attributeMap.size();
		
	}
	
	public static boolean isSafeAttributeName(String name) {
		
		if(name == null)
			return false;
		
		for(char c : UNSAFE_ATTR_NAME_CHARS) {
			
			if(name.indexOf(c) >= 0)
				return false;
			
		}
		
		return true;
		
	}
	
	public boolean hasInvalidAttributeName() {
		
		for(String name : _attributeMap.keySet()) {
			
			if(!isSafeAttributeName(name))
				return true;
			
		}
		
		return false;
		
	}
	
	public DatasetAttributes createSubset(Collection<String> attributeNames) {
		
		DatasetAttributes subset = new DatasetAttributes();
		
		if(attributeNames != null) {
			
			Set<String> namesIntersect = new HashSet<String>(attributeNames);
			namesIntersect.retainAll(getAttributeNames());
			
			for(String attrName : namesIntersect) {
				
				subset.setAttribute(attrName, getAttribute(attrName));
				
			}
			
		}
		
		return subset;
		
	}
	
	public boolean parseAnnotations(IDatabaseObj dbObj) {
		
		if(dbObj == null)
			return false;
		
		_attributeMap.clear();
		
		for(Field field : ReflectionUtility.getFieldsRecursive(dbObj.getClass())) {
			
			if(field.isAnnotationPresent(TableAttribute.class)) {
				
				TableAttribute attr = field.getAnnotation(TableAttribute.class);
				
				if(!attr.readonly()) {
					
					boolean wasAccessible = field.isAccessible();
					field.setAccessible(true);
					
					try {
						_attributeMap.put(attr.column(), field.get(dbObj));
					} catch (IllegalArgumentException | IllegalAccessException e) {
						return false;
					}
					
					field.setAccessible(wasAccessible);
					
				}
				
			}
			
		}
		
		return true;
		
	}
	

}
