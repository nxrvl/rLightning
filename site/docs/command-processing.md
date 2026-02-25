# Command Processing

## Pipeline

1. Client sends RESP-encoded command
2. Network layer parses the RESP message
3. Command dispatcher identifies the command and validates arguments
4. ACL checks verify the user has permission
5. Command handler executes the operation against the storage engine
6. Result is serialized back to RESP format
7. If applicable, command is propagated to replicas and logged to AOF

## Command Dispatch

Each command is registered with its handler, arity, and flags. The dispatcher routes incoming commands to the appropriate handler based on the command name.

## Transaction Support

Commands within a MULTI/EXEC block are queued rather than executed immediately. EXEC executes all queued commands atomically. WATCH provides optimistic locking.

## Script Execution

Lua scripts (EVAL/EVALSHA) and Functions (FCALL) run atomically. During script execution, no other commands are processed, ensuring isolation.
