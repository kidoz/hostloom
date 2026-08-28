# HostLoom.Mapping.Testing

Builds a `HostLoom.Mapping` dispatcher from explicit maps, without a container.

```csharp
IMapper mapper = new TestMapperBuilder()
    .Add<Customer, CustomerDto>(new CustomerMapper())
    .Add<Address, AddressDto>(source => new AddressDto(source.City, source.PostalCode))
    .Build();

var dto = mapper.From(customer).To<CustomerDto>();
```

The core package has no dependency-injection dependency, which is the right boundary but leaves a
unit test that wants the dispatcher building an `IServiceCollection` to get one. This builds the
same contract directly.

Substituting `IMapper` is the other option and a worse one. It needs a substitute per pair, and
each returns what the test told it to rather than what the map would — so the test passes whether
or not the map is correct. A real map, or an inline `Func`, keeps the assertion about behaviour.

Duplicate pairs are rejected here exactly as `MappingBuilder` rejects them, so a test cannot pass
against a composition the container would refuse. A missing pair throws `MappingNotFoundException`
naming both types and the destinations the source *is* mapped to.

Consumers that take a closed `IMapper<TSource, TDestination>` — the shape the `HostLoom.Mapping`
README recommends — need nothing from this package: construct the map class and pass it.
