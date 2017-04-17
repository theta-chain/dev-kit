package com.thetachain.core.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies the methods in a ThetaContract class which can be invoked as SmartContracts.
 * <pre><code>
 * &#064;ThetaContract
 * public class SmartContract {
 *
 * 	&#064;ThetaEndPoint("addCashBalance")
 * 	String addCashBalance() {
 * 		....
 * 	}
 * }
 * </code></pre>
 * @author Pramod Chandersekhar
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ThetaEndPoint {
    String value();
}
