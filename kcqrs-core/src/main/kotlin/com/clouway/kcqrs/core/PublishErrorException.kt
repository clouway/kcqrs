package com.clouway.kcqrs.core

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
class PublishErrorException(val reason: Exception = Exception()) : RuntimeException()