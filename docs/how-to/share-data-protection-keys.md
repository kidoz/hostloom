# Share Data Protection keys across replicas

Use `HostLoom.AspNetCore.DataProtection` when an ASP.NET Core application running on several
pods needs to read cookies or other protected data issued by any replica. The package
configures Microsoft's Redis key repository. A certificate is optional in the current source.
The released 0.22.0 helper requires one.

## Configure the replicas

Reference `HostLoom.AspNetCore.DataProtection` in the web application.
Use the same application name, Redis database, and list key across replicas.

```csharp
using HostLoom.AspNetCore.DataProtection;

builder.Services.AddHostLoomDataProtection(
    applicationName: "catalog-web",
    redisKey: "catalog:production:data-protection",
    redisProfile: "catalog",
    configureRedis: options =>
        options.Configuration = builder.Configuration.GetConnectionString("Redis"));
```

If a cache, lock or rate limiter already configures the same Redis profile, omit
`configureRedis`. Omitting `redisProfile` selects the default connection. The supplied
`redisKey` is the exact list name in the selected database. Use a dedicated connection
profile when key-ring access requires different credentials. Give only the intended
workload access to the ring. Configure durable Redis persistence, backups and protection
against eviction. Losing the ring can invalidate existing cookies.

### Choose how to protect the stored keys

For certificate encryption at rest, add `keyEncryptionCertificate: certificate` to the
registration and make the RSA private key available to every replica. Omitting the argument
(or passing `null`) allows another key encryptor configured through ASP.NET Core Data
Protection to apply. With Redis persistence alone, the cookie remains protected and Data
Protection writes the new key material to Redis without encrypting the key-ring XML.
Microsoft recommends configuring explicit key encryption at rest for production.

If you are using HostLoom 0.22.0 and need certificate-free registration, use Microsoft's
`AddDataProtection().SetApplicationName(...).PersistKeysToStackExchangeRedis(...)`
directly. When reading older certificate-encrypted entries, every replica still needs
access to the corresponding private keys.

## Migrate and verify

Keep the existing Data Protection application discriminator, cookie authentication scheme and
cookie settings. If the current application uses Microsoft's Redis provider, select its exact
database and list key. A filesystem deployment needs its existing key and revocation records
migrated separately, with their decryption material available to every pod. Coordinate the
transition so old and new pods see the same key ring throughout the rollout.

Issue a cookie on one replica and send it to a second replica. Replace the issuing replica
and verify the cookie still authenticates. For an SSO callback, start the flow on one replica
and complete it on another. Key-ring configuration leaves the application's transaction storage
and identity-provider settings under its control.

To rotate the certificate, make the new and previous decryption certificates available to every
replica before any instance writes a key encrypted with the new certificate. Pass the new
certificate for encryption and configure `UnprotectKeysWithAnyCertificate` on the returned
builder for the required decryption certificates. Retain previous private keys while existing
protected data depends on them.

See [Microsoft's Redis key storage guidance](https://learn.microsoft.com/en-us/aspnet/core/security/data-protection/implementation/key-storage-providers?view=aspnetcore-10.0#redis) and
[ASP.NET Core Data Protection configuration](https://learn.microsoft.com/en-us/aspnet/core/security/data-protection/configuration/overview?view=aspnetcore-10.0).
