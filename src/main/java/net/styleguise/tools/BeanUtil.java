package net.styleguise.tools;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Embedded;

public class BeanUtil {
	
	@SuppressWarnings("unchecked")
	public Enum<?> coerceEnum(Class<? extends Enum<?>> enumClazz, String enumValue){
		return Enum.valueOf(enumClazz.asSubclass(Enum.class), enumValue);
	}
	
	public static Field getField(Class<?> clazz, String field){
		try{
			return clazz.getDeclaredField(field);
		}
		catch(NoSuchFieldException e){
			Field[] fields = clazz.getDeclaredFields();
			for( Field f : fields ){
				if( f.isAnnotationPresent(Embedded.class) ){
					Class<?> fieldType = f.getType();
					Field embeddedField = getField(fieldType, field);
					if( embeddedField != null ){
						return embeddedField;
					}
				}
			}
			return null;
		}
	}

	public static List<Class<?>> getParameterizedTypeArguments(Field field){
		try{
			ParameterizedType t = (ParameterizedType)field.getGenericType();
			Type[] args = t.getActualTypeArguments();
			ArrayList<Class<?>> classes = new ArrayList<>();
			for( Type arg : args )
				classes.add((Class<?>)arg);
			return classes;
		}
		catch(SecurityException e) {
			throw new RuntimeException(e);
		}
	}


}
