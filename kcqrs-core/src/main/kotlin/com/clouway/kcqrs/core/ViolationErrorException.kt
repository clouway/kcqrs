package com.clouway.kcqrs.core

/**
 * ValidationErrorException is an exceptional class which is thrown when validation cannot be performed.
 *
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class ViolationErrorException(val errors: Map<String, List<String>>) : RuntimeException()