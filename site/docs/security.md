# Security

## Authentication

### Simple Password

```toml
[security]
requirepass = "your-secure-password"
```

```bash
AUTH your-secure-password
```

### ACL (Access Control Lists)

Fine-grained per-user permissions:

```bash
ACL SETUSER appuser on >password +@read +@write ~app:* &app:*
ACL SETUSER readonly on >readpass +@read ~*
AUTH appuser password
```

See [ACL Commands](commands/acl.md) for full documentation.

## Best Practices

- Always set a password in production
- Use ACL users with minimal permissions per application
- Bind to specific interfaces (not 0.0.0.0) in production
- Use TLS for encrypted connections when available
- Rotate passwords regularly
