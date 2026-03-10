---
title: "ACL"
description: "ACL command reference"
order: 14
category: "commands"
---

# ACL Commands

Access Control Lists (ACLs) provide fine-grained security by controlling which commands, keys, and channels each user can access. rLightning implements the full Redis 7.x ACL system.

## Permission Model

Each user has:

- **Enabled/Disabled status** -- Disabled users cannot authenticate.
- **Passwords** -- One or more passwords for authentication (or `nopass` for passwordless access).
- **Command permissions** -- Allow or deny specific commands or command categories (e.g., `+@read`, `-@dangerous`).
- **Key permissions** -- Restrict access to specific key patterns (e.g., `~user:*`, `%R~readonly:*`).
- **Channel permissions** -- Restrict pub/sub channel access (e.g., `&notifications:*`).

The `default` user is used for connections that do not authenticate.

## AUTH

Synopsis: `AUTH [username] password`

Authenticate to the server. With one argument, authenticates as the `default` user. With two arguments, authenticates as the specified user.

```bash
> AUTH mypassword
OK
> AUTH admin secretpassword
OK
```

## ACL LIST

Synopsis: `ACL LIST`

List all users and their ACL rules in the ACL configuration format.

```bash
> ACL LIST
1) "user default on nopass ~* &* +@all"
2) "user admin on #abc123... ~* &* +@all"
3) "user readonly on #def456... ~* &* +@read -@write"
```

## ACL GETUSER

Synopsis: `ACL GETUSER username`

Return the ACL rules for a specific user.

```bash
> ACL GETUSER admin
 1) "flags"
 2) 1) "on"
 3) "passwords"
 4) 1) "#abc123..."
 5) "commands"
 6) "+@all"
 7) "keys"
 8) "~*"
 9) "channels"
10) "&*"
```

## ACL SETUSER

Synopsis: `ACL SETUSER username [rule [rule ...]]`

Create or modify a user with the given ACL rules.

Common rules:
- `on` / `off` -- Enable or disable the user.
- `>password` -- Add a password.
- `<password` -- Remove a password.
- `nopass` -- Allow passwordless access.
- `resetpass` -- Remove all passwords and disable passwordless.
- `+command` -- Allow a command.
- `-command` -- Deny a command.
- `+@category` -- Allow a command category.
- `-@category` -- Deny a command category.
- `allcommands` / `nocommands` -- Allow or deny all commands.
- `~pattern` -- Allow key access matching the pattern.
- `%R~pattern` -- Allow read-only key access.
- `%W~pattern` -- Allow write-only key access.
- `allkeys` / `resetkeys` -- Allow all keys or reset key permissions.
- `&pattern` -- Allow channel access matching the pattern.
- `allchannels` / `resetchannels` -- Allow all channels or reset channel permissions.

```bash
> ACL SETUSER readonly on >mypass +@read -@write ~cache:* &notifications:*
OK
> ACL SETUSER admin on >adminpass +@all ~* &*
OK
```

## ACL DELUSER

Synopsis: `ACL DELUSER username [username ...]`

Delete one or more users. The `default` user cannot be deleted. Returns the number of users deleted.

```bash
> ACL DELUSER readonly
(integer) 1
```

## ACL WHOAMI

Synopsis: `ACL WHOAMI`

Return the username of the currently authenticated user.

```bash
> ACL WHOAMI
"default"
```

## ACL CAT

Synopsis: `ACL CAT [category]`

List all ACL command categories, or list all commands in a given category.

```bash
> ACL CAT
 1) "read"
 2) "write"
 3) "set"
 4) "sortedset"
 5) "list"
 6) "hash"
 7) "string"
 ...
> ACL CAT read
1) "GET"
2) "MGET"
3) "HGET"
...
```

## ACL GENPASS

Synopsis: `ACL GENPASS [bits]`

Generate a cryptographically secure random password. Default length is 256 bits (64 hex characters).

```bash
> ACL GENPASS
"a1b2c3d4e5f6..."
> ACL GENPASS 128
"a1b2c3d4..."
```

## ACL LOG

Synopsis: `ACL LOG [count | RESET]`

Show recent ACL security events (denied commands). With `RESET`, clears the log.

```bash
> ACL LOG 5
1)  1) "count"
    2) (integer) 1
    3) "reason"
    4) "command"
    5) "context"
    6) "toplevel"
    7) "object"
    8) "SET"
    9) "username"
   10) "readonly"
   ...
> ACL LOG RESET
OK
```

## ACL SAVE

Synopsis: `ACL SAVE`

Save the current ACL configuration to the ACL file (configured via `aclfile` directive).

```bash
> ACL SAVE
OK
```

## ACL LOAD

Synopsis: `ACL LOAD`

Reload the ACL configuration from the ACL file on disk. Replaces the current in-memory ACL state.

```bash
> ACL LOAD
OK
```
