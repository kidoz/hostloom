# HostLoom.AspNetCore.DataProtection

Share ASP.NET Core Data Protection keys across replicas using a HostLoom Redis connection.
Microsoft's Redis repository stores the key ring, and ASP.NET Core manages cookie protection and key rotation.

```csharp
using HostLoom.AspNetCore.DataProtection;

builder.Services.AddHostLoomDataProtection(
    applicationName: "catalog-web",
    redisKey: "catalog:production:data-protection",
    keyEncryptionCertificate: certificate,
    redisProfile: "catalog",
    configureRedis: redis => redis.Configuration = builder.Configuration.GetConnectionString("Redis"));
```

Supply an application-owned RSA certificate with access to its private key on every replica.
Omit `configureRedis` to reuse a profile already configured with `UseRedis`, or omit
`redisProfile` to use the default connection. The profile selects the connection and database.
The supplied Redis key is used verbatim. Use distinct keys and permissions for separate trust domains.
Configure durable Redis persistence and protect the ring from eviction and deletion.

Keep the application name, authentication scheme, key ring, and decryption certificates consistent
across a rolling deployment. When replacing Microsoft's Redis registration, point to the same
database and list key. When moving from a directory, migrate the existing ring before switching
pods. Changing storage alone does not copy keys. Use the returned `IDataProtectionBuilder` with
Microsoft's `UnprotectKeysWithAnyCertificate` to retain access to keys wrapped with prior
certificates during rotation. Keep old private keys available while their protected data is in use.
