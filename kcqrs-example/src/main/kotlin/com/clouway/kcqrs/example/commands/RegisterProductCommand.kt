package com.clouway.kcqrs.example.commands

import com.clouway.kcqrs.core.Command
import java.util.*

/**
 * @author Miroslav Genov (miroslav.genov@clouway.com)
 */
data class RegisterProductCommand(val id: UUID, val name: String) : Command