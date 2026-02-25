# ACL Commands

Access Control Lists (ACL) provide fine-grained security with per-user command permissions, key patterns, and channel restrictions.

## Commands

### ACL SETUSER

Create or modify a user with specific permissions.

```bash
ACL SETUSER username [rule ...]
```

Rules:

- `on` / `off` -- Enable or disable the user
- `>password` -- Add a password
- `<password` -- Remove a password
- `nopass` -- Allow login without password
- `+command` -- Allow a command
- `-command` -- Deny a command
- `+@category` -- Allow all commands in a category
- `-@category` -- Deny all commands in a category
- `allcommands` -- Allow all commands
- `nocommands` -- Deny all commands
- `~pattern` -- Allow key pattern (glob-style)
- `allkeys` -- Allow all keys
- `resetkeys` -- Reset key patterns
- `&pattern` -- Allow pub/sub channel pattern
- `allchannels` -- Allow all channels
- `resetchannels` -- Reset channel patterns
- `reset` -- Reset user to default state

```bash
# Create a read-only user for cache keys
ACL SETUSER cacheuser on >secret +get +mget ~cache:* -@all +@read

# Create an admin user
ACL SETUSER admin on >admin_password allcommands allkeys allchannels
```

### ACL GETUSER

Get user details.

```bash
ACL GETUSER username
```

### ACL DELUSER

Delete users.

```bash
ACL DELUSER username [username ...]
```

### ACL LIST

List all users and their rules.

```bash
ACL LIST
```

### ACL USERS

List all usernames.

```bash
ACL USERS
```

### ACL WHOAMI

Return the current authenticated username.

```bash
ACL WHOAMI
```

### ACL CAT

List command categories, or commands in a category.

```bash
ACL CAT [category]
```

```bash
ACL CAT
# 1) "read"
# 2) "write"
# 3) "set"
# ...

ACL CAT read
# 1) "get"
# 2) "mget"
# ...
```

### ACL GENPASS

Generate a secure random password.

```bash
ACL GENPASS [bits]
```

### ACL LOG

Show recent ACL denials.

```bash
ACL LOG [count|RESET]
```

### ACL SAVE / ACL LOAD

Persist or reload ACL configuration.

```bash
ACL SAVE
ACL LOAD
```

## Authentication

### AUTH

Authenticate with username and password.

```bash
AUTH [username] password
```

```bash
# Default user authentication
AUTH mypassword

# Named user authentication
AUTH cacheuser secret
```

## Use Cases

### Application-Level Isolation

```bash
# Create separate users for different services
ACL SETUSER api_service on >api_key +@read +@write ~api:* &api:*
ACL SETUSER analytics on >analytics_key +@read ~analytics:* ~events:*
ACL SETUSER admin on >admin_key allcommands allkeys allchannels
```

### Read-Only Replica Access

```bash
ACL SETUSER readonly_user on >readonly_pass +@read -@write ~* allchannels
```
