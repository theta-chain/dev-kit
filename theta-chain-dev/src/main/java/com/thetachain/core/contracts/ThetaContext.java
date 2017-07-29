package com.thetachain.core.contracts;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * The context in which the Smart Contract Executes. Gives access to contextual
 * information such as the Proposer Node, The DB Connection etc.
 * 
 * @author Pramod Chandersekhar
 *
 */
public interface ThetaContext {

    /**
     * @return The Node id of the node that proposed the transaction.
     */
    public ThetaNode getProposer();

    /**
     * @return The Database connection to be used for the transaction.
     */
    public Connection getDBConnection();

    /**
     * 
     * @param sql-
     *            The query to execute. Needs to be in JDBC Prepared Statement
     *            format.
     * @param clazz
     *            - The class of the object that needs to be mapped to the
     *            ResultSet.
     * @param args
     *            - Any arguments to the sql query.
     * @return A list of clazz objects. One for each record in the results.
     * @throws SQLException
     */
    public <T> List<T> runPreparedQuery(String sql, Class<T> clazz, Object... args) throws SQLException;

    /**
     * 
     * @param o
     *            - The object to insert. The clazz needs to have a @ThetaName
     *            annotation at the clazz level for the tableName. and at every
     *            field that needs to saved. The @ThetaName at each field gives
     *            the column name.
     * @throws SQLException
     */
    public void insert(Object o) throws SQLException;

    /**
     * 
     * @param o
     *            - The object to update. In addition to the annotations needed
     *            by insert, a @ThetaPrimaryKey is needed at the clazz level.
     * @throws SQLException
     */
    public void update(Object o) throws SQLException;
}
