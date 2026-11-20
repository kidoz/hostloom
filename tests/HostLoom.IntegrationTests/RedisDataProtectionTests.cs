using System.Net;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Xml.Linq;
using HostLoom.AspNetCore.DataProtection;
using HostLoom.RateLimiting.DependencyInjection;
using HostLoom.Redis;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.DataProtection.XmlEncryption;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

public sealed class RedisDataProtectionTests
{
    public static bool Available => RedisAvailability.Redis;
    private const string Profile = "catalog";
    private const int Database = 6;
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task OmittedCertificatePreservesApplicationKeyEncryptor()
    {
        var services = new ServiceCollection();
        var encryptor = Substitute.For<IXmlEncryptor>();
        services.Configure<KeyManagementOptions>(options => options.XmlEncryptor = encryptor);
        services.AddHostLoomDataProtection(
            Profile,
            "catalog:data-protection",
            configureRedis: options => options.Configuration = RedisAvailability.Configuration
        );
        await using var provider = services.BuildServiceProvider();
        Assert.Same(
            encryptor,
            provider.GetRequiredService<IOptions<KeyManagementOptions>>().Value.XmlEncryptor
        );
    }

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(false, false)]
    public async Task CookieSurvivesReplacementAndInteroperatesWithMicrosoft(
        bool hostLoomWritesFirst,
        bool useCertificate
    )
    {
        using var certificate = useCertificate ? CreateCertificate() : null;
        using var redis = await ConnectionMultiplexer.ConnectAsync(RedisAvailability.Configuration);
        var db = redis.GetDatabase(Database);
        var key = $"catalog:test:data-protection:{Guid.NewGuid():N}";
        using var handler = new HttpClientHandler
        {
            UseCookies = false,
            CheckCertificateRevocationList = true,
            AllowAutoRedirect = false,
        };
        using var client = new HttpClient(handler);
        try
        {
            string cookie;
            await using (
                var writer = await StartAsync(hostLoomWritesFirst, key, Profile, certificate, redis)
            )
            {
                using var signIn = await client.GetAsync(
                    new Uri(new Uri(writer.Urls.Single()), "/sign-in"),
                    Token
                );
                signIn.EnsureSuccessStatusCode();
                cookie = signIn.Headers.GetValues("Set-Cookie").Single().Split(';')[0];
            }

            var stored = await db.ListRangeAsync(key);
            var xml = XElement.Parse((string)Assert.Single(stored)!);
            Assert.Equal(
                useCertificate,
                xml.Descendants().Any(e => e.Name.LocalName == "EncryptedData")
            );
            Assert.Equal(
                !useCertificate,
                xml.Descendants().Any(e => e.Name.LocalName == "masterKey")
            );
            Assert.Null(await db.KeyTimeToLiveAsync(key));

            await using var reader = await StartAsync(
                !hostLoomWritesFirst,
                key,
                Profile,
                certificate,
                redis
            );
            Assert.Equal(HttpStatusCode.OK, await ReadCookieAsync(reader, cookie, client));
            await using var isolated = await StartAsync(
                true,
                key,
                "another-app",
                certificate,
                redis
            );
            Assert.Equal(
                HttpStatusCode.Unauthorized,
                await ReadCookieAsync(isolated, cookie, client)
            );
        }
        finally
        {
            await db.KeyDeleteAsync(key);
        }
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task DefaultProfile_RolloverReadsOldKeysAndKeepsBorrowedConnectionOpen()
    {
        using var previous = CreateCertificate();
        using var current = CreateCertificate();
        using var redis = await ConnectionMultiplexer.ConnectAsync(RedisAvailability.Configuration);
        var db = redis.GetDatabase(Database);
        var key = $"catalog:test:data-protection:{Guid.NewGuid():N}";
        try
        {
            string protectedValue;
            await using (var original = RolloverProvider(key, redis, previous))
            {
                protectedValue = original
                    .GetRequiredService<IDataProtectionProvider>()
                    .CreateProtector("catalog-state")
                    .Protect("pending");
            }
            await using (var rotated = RolloverProvider(key, redis, current, previous))
            {
                var protector = rotated
                    .GetRequiredService<IDataProtectionProvider>()
                    .CreateProtector("catalog-state");
                Assert.Equal("pending", protector.Unprotect(protectedValue));
                var now = DateTimeOffset.UtcNow;
                rotated.GetRequiredService<IKeyManager>().CreateNewKey(now, now.AddDays(90));
            }
            // The multiplexer was supplied by the application and still serves after both containers exit.
            var entries = await db.ListRangeAsync(key);
            Assert.Equal(2, entries.Length);
            var storedCertificates = entries
                .Select(value =>
                    XElement
                        .Parse((string)value!)
                        .Descendants()
                        .Single(e => e.Name.LocalName == "X509Certificate")
                        .Value
                )
                .ToArray();
            Assert.Contains(Convert.ToBase64String(previous.RawData), storedCertificates);
            Assert.Contains(Convert.ToBase64String(current.RawData), storedCertificates);
        }
        finally
        {
            await db.KeyDeleteAsync(key);
        }
    }

    private static ServiceProvider RolloverProvider(
        string key,
        IConnectionMultiplexer redis,
        X509Certificate2 certificate,
        params X509Certificate2[] previousCertificates
    )
    {
        var services = new ServiceCollection();
        var builder = services.AddHostLoomDataProtection(
            Profile,
            key,
            certificate,
            configureRedis: options =>
            {
                options.DatabaseIndex = Database;
                options.ConnectionFactory = _ => Task.FromResult<IConnectionMultiplexer>(redis);
            }
        );
        if (previousCertificates.Length > 0)
        {
            builder.UnprotectKeysWithAnyCertificate(previousCertificates);
        }
        return services.BuildServiceProvider();
    }

    private static X509Certificate2 CreateCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=catalog",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1
        );
        return request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-5),
            DateTimeOffset.UtcNow.AddDays(1)
        );
    }

    private static async Task<HttpStatusCode> ReadCookieAsync(
        WebApplication app,
        string cookie,
        HttpClient client
    )
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            new Uri(new Uri(app.Urls.Single()), "/account")
        );
        request.Headers.Add("Cookie", cookie);
        using var response = await client.SendAsync(request, Token);
        return response.StatusCode;
    }

    private static async Task<WebApplication> StartAsync(
        bool useHostLoom,
        string key,
        string appName,
        X509Certificate2? certificate,
        ConnectionMultiplexer redis
    )
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        if (useHostLoom)
        {
            // A preconfigured named profile owns a separate connection for each host.
            builder
                .Services.AddHostLoomRateLimiting(Profile, o => o.Namespace = Profile)
                .UseRedis(o =>
                {
                    o.Configuration = RedisAvailability.Configuration;
                    o.DatabaseIndex = Database;
                });
            if (certificate is null)
                builder.Services.AddHostLoomDataProtection(appName, key, redisProfile: Profile);
            else
                builder.Services.AddHostLoomDataProtection(
                    appName,
                    key,
                    certificate,
                    redisProfile: Profile
                );
        }
        else
        {
            var protection = builder
                .Services.AddDataProtection()
                .SetApplicationName(appName)
                .PersistKeysToStackExchangeRedis(() => redis.GetDatabase(Database), key);
            if (certificate is not null)
                protection.ProtectKeysWithCertificate(certificate);
        }
        builder
            .Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
            .AddCookie();
        var app = builder.Build();
        app.UseAuthentication();
        app.MapGet(
            "/sign-in",
            (HttpContext context) =>
                context.SignInAsync(
                    new ClaimsPrincipal(
                        new ClaimsIdentity([new Claim(ClaimTypes.Name, "reader")], "fixture")
                    )
                )
        );
        app.MapGet(
            "/account",
            (HttpContext context) =>
                context.User.Identity?.Name == "reader" ? Results.Ok() : Results.Unauthorized()
        );
        await app.StartAsync(Token);
        return app;
    }
}
