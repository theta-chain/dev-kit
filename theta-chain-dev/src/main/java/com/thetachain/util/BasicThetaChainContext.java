package com.thetachain.util;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.Timestamp;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.thetachain.core.annotations.ThetaName;
import com.thetachain.core.annotations.ThetaPrimaryKey;
import com.thetachain.core.contracts.ThetaContext;
import com.thetachain.core.contracts.ThetaNode;

public class BasicThetaChainContext implements ThetaContext {
    private static final Object NULL_OBJECT = new Object();
    private Connection connection;
    private ThetaNode proposerNode;

    public BasicThetaChainContext(Connection connection, ThetaNode p) {
	this.connection = connection;
	proposerNode = p;
    }

    public <T> List<T> runPreparedQuery(String sql, Class<T> c, Object... args) throws SQLException {
	List<T> rv = new ArrayList<>();
	try (PreparedStatement stmt = connection.prepareStatement(sql)) {
	    for (int i = 0; i < args.length; i++) {
		stmt.setObject(i + 1, args[i]);
	    }
	    try (ResultSet rs = stmt.executeQuery()) {
		ResultSetWrapper<T> w = new ResultSetWrapper<T>(rs, c);
		while (w.hasNext()) {
		    rv.add(w.getRecord());
		}
	    }
	}
	return rv;
    }

    public void update(Object o) throws SQLException {
	Class<?> clazz = o.getClass();
	ThetaName tn = clazz.getAnnotation(ThetaName.class);
	if (tn == null) {
	    throw new IllegalArgumentException("Class " + clazz + " does not have a ThetaName annotation to denote TableName");
	}
	ThetaPrimaryKey tpkey = clazz.getAnnotation(ThetaPrimaryKey.class);
	if (tpkey == null) {
	    throw new IllegalArgumentException("Primary Key not defined for " + clazz.getName());
	}
	String[] primaryKeyColNames = tpkey.value();
	String tableName = tn.value();
	StringBuilder sb = new StringBuilder("UPDATE ");
	List<Object> args = new ArrayList<>();
	sb.append(tableName);
	sb.append(" SET ");
	try {
	    Map<String, Object> values = getProperties(o);
	    sb.append(" ");
	    for (Map.Entry<String, Object> me : values.entrySet()) {
		if (me.getValue() == NULL_OBJECT || isPrimaryKey(me.getKey(), primaryKeyColNames)) {
		    continue;
		}
		sb.append(me.getKey());
		sb.append(" = ? ,");
		args.add(me.getValue());
	    }
	    if (args.size() == 0) {
		throw new IllegalArgumentException("No Mapped Fields in " + clazz.getName());
	    }
	    sb.setLength(sb.length() - 1);
	    sb.append(" WHERE ");
	    for (int i = 0; i < primaryKeyColNames.length; i++) {
		sb.append(" ");
		sb.append(primaryKeyColNames[i]);
		sb.append(" = ? AND");
		args.add(values.get(primaryKeyColNames[i]));
	    }
	    sb.setLength(sb.length() - 3);
	    String sql = sb.toString();
	    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
		for (int i = 0; i < args.size(); i++) {
		    pstmt.setObject(i + 1, args.get(i));
		}
		pstmt.executeUpdate();
	    }
	} catch (Exception ex) {
	    throw new SQLException(ex);
	}
    }

    private boolean isPrimaryKey(String key, String[] primaryKeyColNames) {
	for (String str : primaryKeyColNames) {
	    if (str.equals(key)) {
		return true;
	    }
	}
	return false;
    }

    public void insert(Object o) throws SQLException {
	Class<?> clazz = o.getClass();
	ThetaName tn = clazz.getAnnotation(ThetaName.class);
	if (tn == null) {
	    throw new IllegalArgumentException("Class " + clazz + " does not have a ThetaName annotation to denote TableName");
	}
	String tableName = tn.value();
	StringBuilder sb = new StringBuilder("INSERT INTO ");
	StringBuilder sbValues = new StringBuilder("(");
	List<Object> args = new ArrayList<>();
	sb.append(tableName);
	try {
	    Map<String, Object> values = getProperties(o);
	    sb.append(" (");
	    for (Map.Entry<String, Object> me : values.entrySet()) {
		if (me.getValue() == NULL_OBJECT) {
		    continue;
		}
		sb.append(me.getKey());
		sb.append(',');
		sbValues.append("?,");
		args.add(me.getValue());
	    }
	    if (args.size() == 0) {
		throw new IllegalArgumentException("No Mapped Fields in " + clazz.getName());
	    }
	    sb.setLength(sb.length() - 1);
	    sbValues.setLength(sbValues.length() - 1);
	    sb.append(") VALUES ");
	    sb.append(sbValues);
	    sb.append(")");
	    String sql = sb.toString();
	    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
		for (int i = 0; i < args.size(); i++) {
		    pstmt.setObject(i + 1, args.get(i));
		}
		pstmt.executeUpdate();
	    }
	} catch (Exception ex) {
	    throw new SQLException(ex);
	}
    }

    private static Map<String, Object> getProperties(Object o) throws Exception {
	Map<String, Object> rv = new HashMap<>();
	Class<?> nc = o.getClass();
	while (nc != Object.class) {
	    for (Field f : nc.getDeclaredFields()) {
		ThetaName tn = f.getAnnotation(ThetaName.class);
		if (tn == null) {
		    continue;
		}
		String name = tn.value();
		f.setAccessible(true);
		Object val = f.get(o);
		rv.put(name, val == null ? NULL_OBJECT : val);
	    }
	    nc = nc.getSuperclass();
	}
	return rv;
    }

    @Override
    public ThetaNode getProposer() {
	return proposerNode;
    }

    @Override
    public Connection getDBConnection() {
	return connection;
    }
    
}

class ResultSetWrapper<T> {

    private ResultSet resultSet;
    private Class<T> clazz;

    private int colCount;
    private ValueSetter[] valueSetters;

    public ResultSetWrapper(ResultSet rs, Class<T> c) throws SQLException {
	this.resultSet = rs;
	this.clazz = c;
	try {
	    ValueSetter nullSetter = (r, o) -> {
	    };
	    ResultSetMetaData rsmd = resultSet.getMetaData();
	    colCount = rsmd.getColumnCount();
	    valueSetters = new ValueSetter[colCount];
	    loadFieldSetters(rsmd, c);
	    BeanInfo bi = Introspector.getBeanInfo(clazz, java.lang.Object.class);
	    PropertyDescriptor[] pds = bi.getPropertyDescriptors();
	    Map<String, PropertyDescriptor> pdMap = new HashMap<>();
	    for (PropertyDescriptor pd : pds) {
		pdMap.put(fixName(pd.getName()), pd);
	    }
	    for (int i = 0; i < colCount; i++) {
		if (valueSetters[i] != null) {
		    continue;
		}
		String colName = fixName(rsmd.getColumnName(i + 1));
		PropertyDescriptor pd = pdMap.get(colName);
		if (pd == null) {
		    valueSetters[i] = nullSetter;
		} else {
		    valueSetters[i] = getPropertySetter(i + 1, pd);
		}
	    }
	} catch (Exception ex) {
	    throw new SQLException(ex);
	}
    }

    private void loadFieldSetters(ResultSetMetaData rsmd, Class<T> c) throws SQLException {
	Map<String, Field> fieldMap = new HashMap<>();
	Class<?> nc = c;
	while (nc != Object.class) {
	    for (Field f : nc.getDeclaredFields()) {
		ThetaName tn = f.getAnnotation(ThetaName.class);
		if (tn == null) {
		    continue;
		}
		String name = tn.value();
		fieldMap.put(name, f);
	    }
	    nc = nc.getSuperclass();
	}
	for (int i = 0; i < colCount; i++) {
	    String colName = rsmd.getColumnName(i + 1);
	    Field f = fieldMap.get(colName);
	    if (f != null) {
		ResultSetGetter rsg = getResultSetGetter(f.getType());
		valueSetters[i] = new FieldSetter(i + 1, f, rsg);
	    }
	}
    }

    private ValueSetter getPropertySetter(int colIndex, PropertyDescriptor pd) {
	Class<?> propClass = pd.getPropertyType();
	ResultSetGetter resultSetGetter = getResultSetGetter(propClass);
	return new PropertySetter(colIndex, pd.getWriteMethod(), resultSetGetter);
    }

    private ResultSetGetter getResultSetGetter(Class<?> propClass) {
	ResultSetGetter resultSetGetter = null;
	if (propClass == String.class) {
	    resultSetGetter = (rs, ci) -> rs.getString(ci);
	} else if (propClass == BigDecimal.class) {
	    resultSetGetter = (rs, ci) -> rs.getBigDecimal(ci);
	} else if (propClass == BigInteger.class) {
	    resultSetGetter = (rs, ci) -> {
		BigDecimal bd = rs.getBigDecimal(ci);
		return bd == null ? null : bd.toBigInteger();
	    };
	} else if (propClass == java.util.Date.class) {
	    resultSetGetter = (rs, ci) -> {
		java.sql.Date tmp = rs.getDate(ci);
		return tmp == null ? null : new java.util.Date(tmp.getTime());
	    };
	} else if (propClass == Timestamp.class) {
	    resultSetGetter = (rs, ci) -> rs.getTimestamp(ci);
	} else {
	    throw new IllegalArgumentException("ORM Does not handle property Type = " + propClass.getName());
	}
	return resultSetGetter;
    }

    private String fixName(String name) {
	name = name.replaceAll("_", "");
	name = name.toLowerCase();
	return name;
    }

    public T getRecord() throws SQLException {
	try {
	    T rv = clazz.newInstance();
	    for (int i = 0; i < colCount; i++) {
		valueSetters[i].setValue(resultSet, rv);
	    }
	    return rv;
	} catch (Exception ex) {
	    throw new SQLException(ex);
	}
    }

    public boolean hasNext() throws SQLException {
	return resultSet.next();
    }

}

interface ValueSetter {
    public void setValue(ResultSet rs, Object o) throws SQLException;
}

/**
 * This is a new interface instead of BiConsumer due to the SQLException in the
 * throws clause
 * 
 * @author pramod
 *
 */
interface ResultSetGetter {
    public Object getValue(ResultSet rs, int colIndex) throws SQLException;
}

class PropertySetter implements ValueSetter {

    private int colIndex;
    private Method writeMethod;
    private ResultSetGetter resultSetGetter;

    public PropertySetter(int colIndex, Method writeMethod, ResultSetGetter resultSetGetter) {
	this.colIndex = colIndex;
	this.writeMethod = writeMethod;
	this.resultSetGetter = resultSetGetter;
    }

    @Override
    public void setValue(ResultSet rs, Object o) throws SQLException {
	Object val = resultSetGetter.getValue(rs, colIndex);
	try {
	    writeMethod.invoke(o, val);
	} catch (InvocationTargetException ex) {
	    throw new SQLException(ex.getTargetException());
	} catch (Exception ex) {
	    throw new SQLException(ex);
	}
    }

}

class FieldSetter implements ValueSetter {

    private int colIndex;
    private Field field;
    private ResultSetGetter resultSetGetter;

    public FieldSetter(int colIndex, Field field, ResultSetGetter resultSetGetter) {
	this.colIndex = colIndex;
	this.field = field;
	this.field.setAccessible(true);
	this.resultSetGetter = resultSetGetter;
    }

    @Override
    public void setValue(ResultSet rs, Object o) throws SQLException {
	Object val = resultSetGetter.getValue(rs, colIndex);
	try {
	    field.set(o, val);
	} catch (Exception ex) {
	    throw new SQLException(ex);
	}
    }

}
