package com.thetachain.core.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies the classes which are entry points for SmartContract code.
 * <code>
 * &#64;ThetaContract
 * public class SmartContract {
 * }
 * </code>
 * @author Pramod Chandersekhar
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ThetaContract {
}
