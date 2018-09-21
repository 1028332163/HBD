package com.esoft.wms.utils;
    
import java.io.IOException;
import java.util.Properties;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

public class Config extends PropertyPlaceholderConfigurer {

    public static Properties props;

    @Override
    protected void loadProperties(Properties property) throws IOException {
        super.loadProperties(property);
        props = property;
    }
    
    public static String getValue(String key) {
    	return props.getProperty(key);
    }
    
    
}
