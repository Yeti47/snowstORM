package net.yetibyte.snowstorm;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
public @interface TableAttribute {

	public String column();
	
	public boolean readonly() default false;
	
}
