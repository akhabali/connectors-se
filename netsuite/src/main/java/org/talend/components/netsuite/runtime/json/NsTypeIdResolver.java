package org.talend.components.netsuite.runtime.json;

import org.talend.components.netsuite.runtime.model.BasicMetaData;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 *
 */
public class NsTypeIdResolver extends TypeIdResolverBase {

    private BasicMetaData basicMetaData;

    protected NsTypeIdResolver(JavaType baseType, TypeFactory typeFactory, BasicMetaData basicMetaData) {
        super(baseType, typeFactory);

        this.basicMetaData = basicMetaData;
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
        Class<?> clazz = basicMetaData.getTypeClass(id);
        if (clazz == null) {
            return null;
        }
        JavaType javaType = SimpleType.construct(clazz);
        return javaType;
    }

    @Override
    public String idFromValue(Object value) {
        return value.getClass().getSimpleName();
    }

    @Override
    public String idFromValueAndType(Object value, Class<?> suggestedType) {
        return suggestedType.getSimpleName();
    }

    @Override
    public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.NAME;
    }
}
