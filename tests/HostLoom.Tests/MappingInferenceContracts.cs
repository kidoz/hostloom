// Fixtures for the registration-ergonomics test in MappingTests. The contracts and the map class
// live in separate namespaces on purpose: inferring the pair means a registration names only the
// map class, so the file that registers it never imports the contract namespace. MappingTests.cs
// imports Maps and deliberately does not import Contracts — that omission is the assertion.

namespace HostLoom.Tests.MappingInference.Contracts
{
    /// <summary>A source contract the registration file must never need to name.</summary>
    public sealed record RegistrationRequest(string Value);

    /// <summary>A destination contract the registration file must never need to name.</summary>
    public sealed record RegistrationDto(string Value);
}

namespace HostLoom.Tests.MappingInference.Maps
{
    using global::HostLoom.Mapping;
    using global::HostLoom.Tests.MappingInference.Contracts;

    /// <summary>Declares its pair on the interface, which is the only place it should be stated.</summary>
    public sealed class RegistrationRequestMapper : IMapper<RegistrationRequest, RegistrationDto>
    {
        public RegistrationDto Map(RegistrationRequest source) => new(source.Value);
    }
}
