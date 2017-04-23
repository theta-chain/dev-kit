package com.thetachain.core.contracts;

import java.sql.Connection;

/**
 * The context in which the Smart Contract Executes. Gives access to contextual information such as the Proposer Node, The DB Connection etc.
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
}
