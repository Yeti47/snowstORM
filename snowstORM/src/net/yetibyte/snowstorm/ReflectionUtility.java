package net.yetibyte.snowstorm;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public abstract class ReflectionUtility {
	
	private static Collection<Field> getFieldsRecursive(List<Field> fields, Class<?> type) {
		
		fields.addAll(Arrays.asList(type.getDeclaredFields()));
		
		if(type.getSuperclass() != null)
			getFieldsRecursive(fields, type.getSuperclass());
		
		return fields;
		
	}
	
	public static Collection<Field> getFieldsRecursive(Class<?> type) {
		
		return getFieldsRecursive(new ArrayList<Field>(), type);
		
	}
	
	private static Collection<Field> getFieldsWithAnnotation(List<Field> fields, Class<?> type, Class<? extends Annotation> annotationType) {
		
		for(Field field : type.getDeclaredFields()) {
			
			if(field.isAnnotationPresent(annotationType))
				fields.add(field);
			
		}
		
		if(type.getSuperclass() != null)
			getFieldsWithAnnotation(fields, type.getSuperclass(), annotationType);
		
		return fields;
		
	}
	
	public static Collection<Field> getFieldsWithAnnotation(Class<?> type, Class<? extends Annotation> annotationType) {
		
		return getFieldsWithAnnotation(new ArrayList<Field>(), type, annotationType);
		
	}

}
