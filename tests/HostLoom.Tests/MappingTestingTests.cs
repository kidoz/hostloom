using HostLoom.Mapping;
using HostLoom.Mapping.Testing;
using Xunit;

namespace HostLoom.Tests;

public sealed class MappingTestingTests
{
    [Fact]
    public void A_composed_dispatcher_maps_through_instances_and_delegates()
    {
        IMapper mapper = new TestMapperBuilder()
            .Add<Customer, CustomerDto>(new CustomerMapper())
            .Add<Address, AddressDto>(source => new AddressDto(source.City, source.PostalCode))
            .Build();

        Assert.Equal("Ada", mapper.From(new Customer("Ada")).To<CustomerDto>().DisplayName);
        Assert.Equal(
            new AddressDto("Paris", "75001"),
            mapper.Map<Address, AddressDto>(new Address("Paris", "75001"))
        );
    }

    [Fact]
    public void A_missing_pair_reports_both_types_and_the_registered_destinations()
    {
        IMapper mapper = new TestMapperBuilder()
            .Add<Customer, CustomerDto>(new CustomerMapper())
            .Build();

        var exception = Assert.Throws<MappingNotFoundException>(() =>
            mapper.Map<Customer, AddressDto>(new Customer("Ada"))
        );

        Assert.Equal(typeof(Customer), exception.SourceType);
        Assert.Equal(typeof(AddressDto), exception.DestinationType);
        Assert.Equal([typeof(CustomerDto)], exception.RegisteredDestinations);
    }

    [Fact]
    public void A_duplicate_pair_is_rejected_as_the_container_builder_rejects_it()
    {
        // A test that composes a mapping the container would refuse is a test that can pass while
        // the application it stands for cannot start.
        TestMapperBuilder builder = new TestMapperBuilder().Add<Customer, CustomerDto>(
            new CustomerMapper()
        );

        Assert.Throws<InvalidOperationException>(() =>
            builder.Add<Customer, CustomerDto>(source => new CustomerDto(source.Name))
        );
    }

    [Fact]
    public void A_built_dispatcher_is_unaffected_by_later_additions()
    {
        TestMapperBuilder builder = new TestMapperBuilder().Add<Customer, CustomerDto>(
            new CustomerMapper()
        );
        IMapper mapper = builder.Build();
        builder.Add<Address, AddressDto>(source => new AddressDto(source.City, source.PostalCode));

        Assert.Throws<MappingNotFoundException>(() =>
            mapper.Map<Address, AddressDto>(new Address("Paris", "75001"))
        );
    }

    [Fact]
    public void Null_arguments_are_rejected()
    {
        var builder = new TestMapperBuilder();

        Assert.Throws<ArgumentNullException>(() =>
            builder.Add<Customer, CustomerDto>((IMapper<Customer, CustomerDto>)null!)
        );
        Assert.Throws<ArgumentNullException>(() =>
            builder.Add<Customer, CustomerDto>((Func<Customer, CustomerDto>)null!)
        );
        Assert.Throws<ArgumentNullException>(() =>
            builder.Build().Map<Customer, CustomerDto>(null!)
        );
    }

    private sealed record Customer(string Name);

    private sealed record CustomerDto(string DisplayName);

    private sealed record Address(string City, string PostalCode);

    private sealed record AddressDto(string City, string PostalCode);

    private sealed class CustomerMapper : IMapper<Customer, CustomerDto>
    {
        public CustomerDto Map(Customer source) => new(source.Name);
    }
}
