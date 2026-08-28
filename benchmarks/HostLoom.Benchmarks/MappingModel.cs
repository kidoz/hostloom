using AutoMapper;
using HostLoom.Mapping;
using HostLoom.Mapping.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Benchmarks;

// ---- Flat shape -----------------------------------------------------------------------------
//
// Eight scalar members with identical names on both sides. This is deliberately AutoMapper's
// best case: pure convention, no ForMember, no custom resolver, nothing its configuration has
// to look up at map time beyond the compiled plan.

/// <summary>An everyday flat contract: eight scalars, no nesting.</summary>
public sealed record Customer(
    Guid Id,
    string FirstName,
    string LastName,
    string Email,
    string Country,
    int LoyaltyPoints,
    bool IsActive,
    DateTimeOffset RegisteredAt
);

/// <summary>The flat destination. Member names match the source exactly.</summary>
public sealed record CustomerDto(
    Guid Id,
    string FirstName,
    string LastName,
    string Email,
    string Country,
    int LoyaltyPoints,
    bool IsActive,
    DateTimeOffset RegisteredAt
);

// ---- Nested shape ---------------------------------------------------------------------------
//
// A nested object plus a three-element child collection: the shape a real message contract
// tends to have, and the one where a mapper's collection and recursion machinery shows up.

/// <summary>A nested child object.</summary>
public sealed record Address(string Street, string City, string PostalCode, string Country);

/// <summary>The nested child destination.</summary>
public sealed record AddressDto(string Street, string City, string PostalCode, string Country);

/// <summary>One element of the child collection.</summary>
public sealed record InvoiceLine(string Sku, string Description, int Quantity, decimal UnitPrice);

/// <summary>The child collection destination element.</summary>
public sealed record InvoiceLineDto(
    string Sku,
    string Description,
    int Quantity,
    decimal UnitPrice
);

/// <summary>A nested contract: one child object and one child collection.</summary>
public sealed record Invoice(
    Guid Id,
    string Reference,
    Address ShipTo,
    IReadOnlyList<InvoiceLine> Lines,
    decimal Total,
    string Currency,
    DateTimeOffset PlacedAt
);

/// <summary>The nested destination, mirroring <see cref="Invoice"/> member for member.</summary>
public sealed record InvoiceDto(
    Guid Id,
    string Reference,
    AddressDto ShipTo,
    IReadOnlyList<InvoiceLineDto> Lines,
    decimal Total,
    string Currency,
    DateTimeOffset PlacedAt
);

// ---- HostLoom maps --------------------------------------------------------------------------

/// <summary>The flat map as HostLoom expects it: an ordinary, compiler-checked C# class.</summary>
public sealed class CustomerMapper : IMapper<Customer, CustomerDto>
{
    public CustomerDto Map(Customer source) =>
        new(
            source.Id,
            source.FirstName,
            source.LastName,
            source.Email,
            source.Country,
            source.LoyaltyPoints,
            source.IsActive,
            source.RegisteredAt
        );
}

/// <summary>The nested child map.</summary>
public sealed class AddressMapper : IMapper<Address, AddressDto>
{
    public AddressDto Map(Address source) =>
        new(source.Street, source.City, source.PostalCode, source.Country);
}

/// <summary>The child collection element map.</summary>
public sealed class InvoiceLineMapper : IMapper<InvoiceLine, InvoiceLineDto>
{
    public InvoiceLineDto Map(InvoiceLine source) =>
        new(source.Sku, source.Description, source.Quantity, source.UnitPrice);
}

/// <summary>
/// The nested map, composing the two child maps through constructor injection exactly as the
/// package README recommends. The child collection is walked by hand — HostLoom has no
/// collection mapping feature, so the loop is what an application actually writes.
/// </summary>
public sealed class InvoiceMapper(
    IMapper<Address, AddressDto> addressMapper,
    IMapper<InvoiceLine, InvoiceLineDto> lineMapper
) : IMapper<Invoice, InvoiceDto>
{
    public InvoiceDto Map(Invoice source)
    {
        var lines = new InvoiceLineDto[source.Lines.Count];
        for (var index = 0; index < lines.Length; index++)
        {
            lines[index] = lineMapper.Map(source.Lines[index]);
        }

        return new InvoiceDto(
            source.Id,
            source.Reference,
            addressMapper.Map(source.ShipTo),
            lines,
            source.Total,
            source.Currency,
            source.PlacedAt
        );
    }
}

// ---- Shared fixtures ------------------------------------------------------------------------

/// <summary>
/// Fixed sample data. Every value is a literal so two runs on the same machine compare against
/// the same bytes rather than against a fresh <see cref="Guid"/> or clock reading.
/// </summary>
internal static class MappingData
{
    public static Customer Customer { get; } =
        new(
            Guid.Parse("a2b8f9e0-1234-4cde-9f00-56789abcdef0"),
            "Ada",
            "Lovelace",
            "ada@example.com",
            "GB",
            4200,
            true,
            new DateTimeOffset(2026, 8, 26, 10, 0, 0, TimeSpan.Zero)
        );

    public static Invoice Invoice { get; } =
        new(
            Guid.Parse("b3c9a0f1-2345-4def-8a11-6789abcdef01"),
            "ORD-100427",
            new Address("1 Rue de Rivoli", "Paris", "75001", "FR"),
            [
                new InvoiceLine("SKU-1", "Widget", 2, 9.99m),
                new InvoiceLine("SKU-2", "Gadget", 1, 24.50m),
                new InvoiceLine("SKU-3", "Doohickey", 5, 3.25m),
            ],
            68.23m,
            "EUR",
            new DateTimeOffset(2026, 8, 26, 10, 5, 0, TimeSpan.Zero)
        );

    /// <summary>Builds a batch of distinct customers so no map sees the same instance twice.</summary>
    public static Customer[] CustomerBatch(int count)
    {
        var batch = new Customer[count];
        for (var index = 0; index < count; index++)
        {
            batch[index] = Customer with
            {
                LoyaltyPoints = index,
                Email = $"customer{index}@example.com",
            };
        }

        return batch;
    }
}

/// <summary>
/// The registration each side needs, kept in one place so the steady-state suites and the
/// cold-start suite measure exactly the same configuration.
/// </summary>
internal static class MappingRegistration
{
    /// <summary>Registers the four HostLoom maps and the scoped dispatcher.</summary>
    public static IServiceCollection AddHostLoomMaps(IServiceCollection services)
    {
        services.AddHostLoomMapping(mapping =>
            mapping
                .Add<Customer, CustomerDto, CustomerMapper>()
                .Add<Address, AddressDto, AddressMapper>()
                .Add<InvoiceLine, InvoiceLineDto, InvoiceLineMapper>()
                .Add<Invoice, InvoiceDto, InvoiceMapper>()
        );
        return services;
    }

    /// <summary>
    /// The same four maps registered through inferred pairs, so the cost of reading each pair off
    /// the map class's interface can be compared against restating it in the call.
    /// </summary>
    public static IServiceCollection AddHostLoomMapsInferred(IServiceCollection services)
    {
        services.AddHostLoomMapping(mapping =>
            mapping
                .Add<CustomerMapper>()
                .Add<AddressMapper>()
                .Add<InvoiceLineMapper>()
                .Add<InvoiceMapper>()
        );
        return services;
    }

    /// <summary>
    /// The same four maps registered through factories, which is how a generic map class is
    /// closed. The factory replaces the container's own activator, so this measures what that
    /// substitution costs at registration and at resolve.
    /// </summary>
    public static IServiceCollection AddHostLoomMapsFactory(IServiceCollection services)
    {
        services.AddHostLoomMapping(mapping =>
            mapping
                .Add<Customer, CustomerDto>(_ => new CustomerMapper())
                .Add<Address, AddressDto>(_ => new AddressMapper())
                .Add<InvoiceLine, InvoiceLineDto>(_ => new InvoiceLineMapper())
                .Add<Invoice, InvoiceDto>(provider => new InvoiceMapper(
                    provider.GetRequiredService<IMapper<Address, AddressDto>>(),
                    provider.GetRequiredService<IMapper<InvoiceLine, InvoiceLineDto>>()
                ))
        );
        return services;
    }

    /// <summary>Declares the same four maps to AutoMapper.</summary>
    public static void ConfigureAutoMapper(IMapperConfigurationExpression configuration)
    {
        configuration.CreateMap<Customer, CustomerDto>();
        configuration.CreateMap<Address, AddressDto>();
        configuration.CreateMap<InvoiceLine, InvoiceLineDto>();
        configuration.CreateMap<Invoice, InvoiceDto>();
    }

    /// <summary>
    /// Registers AutoMapper. It resolves an <see cref="ILoggerFactory"/> during configuration,
    /// so the container must supply one; the null factory keeps logging out of the measurement.
    /// </summary>
    public static IServiceCollection AddAutoMapperMaps(IServiceCollection services)
    {
        services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddAutoMapper(ConfigureAutoMapper);
        return services;
    }

    /// <summary>
    /// Fails the run if the two libraries do not produce the same destination values, so a
    /// silently incomplete map on either side can never be reported as a faster one. Called from
    /// <c>GlobalSetup</c>, never from a measured iteration.
    /// </summary>
    public static void VerifyEquivalence(
        IMapper<Customer, CustomerDto> hostLoomCustomer,
        IMapper<Invoice, InvoiceDto> hostLoomInvoice,
        AutoMapper.IMapper autoMapper
    )
    {
        // Catches a destination member AutoMapper's conventions never filled in. The HostLoom
        // side has no equivalent check because an unset member is a compiler error there.
        new MapperConfiguration(
            ConfigureAutoMapper,
            NullLoggerFactory.Instance
        ).AssertConfigurationIsValid();

        var expectedCustomer = hostLoomCustomer.Map(MappingData.Customer);
        var actualCustomer = autoMapper.Map<Customer, CustomerDto>(MappingData.Customer);
        Require(expectedCustomer == actualCustomer, "flat destinations differ");

        var expectedInvoice = hostLoomInvoice.Map(MappingData.Invoice);
        var actualInvoice = autoMapper.Map<Invoice, InvoiceDto>(MappingData.Invoice);

        // Compared member by member because the record's synthesized equality falls back to
        // reference equality for the child collection, which is an array on one side and a
        // List<T> on the other.
        Require(expectedInvoice.Id == actualInvoice.Id, "invoice Id differs");
        Require(expectedInvoice.Reference == actualInvoice.Reference, "invoice Reference differs");
        Require(expectedInvoice.ShipTo == actualInvoice.ShipTo, "invoice ShipTo differs");
        Require(expectedInvoice.Total == actualInvoice.Total, "invoice Total differs");
        Require(expectedInvoice.Currency == actualInvoice.Currency, "invoice Currency differs");
        Require(expectedInvoice.PlacedAt == actualInvoice.PlacedAt, "invoice PlacedAt differs");
        Require(
            expectedInvoice.Lines.SequenceEqual(actualInvoice.Lines),
            "invoice Lines content differs"
        );
    }

    private static void Require(bool condition, string what)
    {
        if (condition is false)
        {
            throw new InvalidOperationException(
                $"HostLoom and AutoMapper disagree: {what}. The suites are not comparing "
                    + "equivalent work, so the results would be meaningless."
            );
        }
    }

    /// <summary>Builds AutoMapper without a container, with every execution plan compiled.</summary>
    public static AutoMapper.IMapper CreateAutoMapper()
    {
        var configuration = new MapperConfiguration(
            ConfigureAutoMapper,
            NullLoggerFactory.Instance
        );
        configuration.CompileMappings();
        return configuration.CreateMapper();
    }
}
