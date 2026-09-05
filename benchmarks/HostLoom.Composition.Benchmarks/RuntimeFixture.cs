using HostLoom.Composition;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition.Benchmarks;

internal static partial class RuntimeFixture
{
    [CompositionRules(nameof(CreatePlan))]
    private static void Declare(CompositionRuleBuilder rules)
    {
        rules
            .AddClasses()
            .AssignableTo(typeof(ICatalog<>))
            .AsImplementedInterfaces()
            .WithScopedLifetime()
            .ExpectOne()
            .ExpectExactly(100);
    }

    public static partial CompositionPlan CreatePlan();

    internal static IServiceCollection Handwritten()
    {
        var services = new ServiceCollection();
        services.AddScoped<ICatalog<Item000>, Catalog000>();
        services.AddScoped<ICatalog<Item001>, Catalog001>();
        services.AddScoped<ICatalog<Item002>, Catalog002>();
        services.AddScoped<ICatalog<Item003>, Catalog003>();
        services.AddScoped<ICatalog<Item004>, Catalog004>();
        services.AddScoped<ICatalog<Item005>, Catalog005>();
        services.AddScoped<ICatalog<Item006>, Catalog006>();
        services.AddScoped<ICatalog<Item007>, Catalog007>();
        services.AddScoped<ICatalog<Item008>, Catalog008>();
        services.AddScoped<ICatalog<Item009>, Catalog009>();
        services.AddScoped<ICatalog<Item010>, Catalog010>();
        services.AddScoped<ICatalog<Item011>, Catalog011>();
        services.AddScoped<ICatalog<Item012>, Catalog012>();
        services.AddScoped<ICatalog<Item013>, Catalog013>();
        services.AddScoped<ICatalog<Item014>, Catalog014>();
        services.AddScoped<ICatalog<Item015>, Catalog015>();
        services.AddScoped<ICatalog<Item016>, Catalog016>();
        services.AddScoped<ICatalog<Item017>, Catalog017>();
        services.AddScoped<ICatalog<Item018>, Catalog018>();
        services.AddScoped<ICatalog<Item019>, Catalog019>();
        services.AddScoped<ICatalog<Item020>, Catalog020>();
        services.AddScoped<ICatalog<Item021>, Catalog021>();
        services.AddScoped<ICatalog<Item022>, Catalog022>();
        services.AddScoped<ICatalog<Item023>, Catalog023>();
        services.AddScoped<ICatalog<Item024>, Catalog024>();
        services.AddScoped<ICatalog<Item025>, Catalog025>();
        services.AddScoped<ICatalog<Item026>, Catalog026>();
        services.AddScoped<ICatalog<Item027>, Catalog027>();
        services.AddScoped<ICatalog<Item028>, Catalog028>();
        services.AddScoped<ICatalog<Item029>, Catalog029>();
        services.AddScoped<ICatalog<Item030>, Catalog030>();
        services.AddScoped<ICatalog<Item031>, Catalog031>();
        services.AddScoped<ICatalog<Item032>, Catalog032>();
        services.AddScoped<ICatalog<Item033>, Catalog033>();
        services.AddScoped<ICatalog<Item034>, Catalog034>();
        services.AddScoped<ICatalog<Item035>, Catalog035>();
        services.AddScoped<ICatalog<Item036>, Catalog036>();
        services.AddScoped<ICatalog<Item037>, Catalog037>();
        services.AddScoped<ICatalog<Item038>, Catalog038>();
        services.AddScoped<ICatalog<Item039>, Catalog039>();
        services.AddScoped<ICatalog<Item040>, Catalog040>();
        services.AddScoped<ICatalog<Item041>, Catalog041>();
        services.AddScoped<ICatalog<Item042>, Catalog042>();
        services.AddScoped<ICatalog<Item043>, Catalog043>();
        services.AddScoped<ICatalog<Item044>, Catalog044>();
        services.AddScoped<ICatalog<Item045>, Catalog045>();
        services.AddScoped<ICatalog<Item046>, Catalog046>();
        services.AddScoped<ICatalog<Item047>, Catalog047>();
        services.AddScoped<ICatalog<Item048>, Catalog048>();
        services.AddScoped<ICatalog<Item049>, Catalog049>();
        services.AddScoped<ICatalog<Item050>, Catalog050>();
        services.AddScoped<ICatalog<Item051>, Catalog051>();
        services.AddScoped<ICatalog<Item052>, Catalog052>();
        services.AddScoped<ICatalog<Item053>, Catalog053>();
        services.AddScoped<ICatalog<Item054>, Catalog054>();
        services.AddScoped<ICatalog<Item055>, Catalog055>();
        services.AddScoped<ICatalog<Item056>, Catalog056>();
        services.AddScoped<ICatalog<Item057>, Catalog057>();
        services.AddScoped<ICatalog<Item058>, Catalog058>();
        services.AddScoped<ICatalog<Item059>, Catalog059>();
        services.AddScoped<ICatalog<Item060>, Catalog060>();
        services.AddScoped<ICatalog<Item061>, Catalog061>();
        services.AddScoped<ICatalog<Item062>, Catalog062>();
        services.AddScoped<ICatalog<Item063>, Catalog063>();
        services.AddScoped<ICatalog<Item064>, Catalog064>();
        services.AddScoped<ICatalog<Item065>, Catalog065>();
        services.AddScoped<ICatalog<Item066>, Catalog066>();
        services.AddScoped<ICatalog<Item067>, Catalog067>();
        services.AddScoped<ICatalog<Item068>, Catalog068>();
        services.AddScoped<ICatalog<Item069>, Catalog069>();
        services.AddScoped<ICatalog<Item070>, Catalog070>();
        services.AddScoped<ICatalog<Item071>, Catalog071>();
        services.AddScoped<ICatalog<Item072>, Catalog072>();
        services.AddScoped<ICatalog<Item073>, Catalog073>();
        services.AddScoped<ICatalog<Item074>, Catalog074>();
        services.AddScoped<ICatalog<Item075>, Catalog075>();
        services.AddScoped<ICatalog<Item076>, Catalog076>();
        services.AddScoped<ICatalog<Item077>, Catalog077>();
        services.AddScoped<ICatalog<Item078>, Catalog078>();
        services.AddScoped<ICatalog<Item079>, Catalog079>();
        services.AddScoped<ICatalog<Item080>, Catalog080>();
        services.AddScoped<ICatalog<Item081>, Catalog081>();
        services.AddScoped<ICatalog<Item082>, Catalog082>();
        services.AddScoped<ICatalog<Item083>, Catalog083>();
        services.AddScoped<ICatalog<Item084>, Catalog084>();
        services.AddScoped<ICatalog<Item085>, Catalog085>();
        services.AddScoped<ICatalog<Item086>, Catalog086>();
        services.AddScoped<ICatalog<Item087>, Catalog087>();
        services.AddScoped<ICatalog<Item088>, Catalog088>();
        services.AddScoped<ICatalog<Item089>, Catalog089>();
        services.AddScoped<ICatalog<Item090>, Catalog090>();
        services.AddScoped<ICatalog<Item091>, Catalog091>();
        services.AddScoped<ICatalog<Item092>, Catalog092>();
        services.AddScoped<ICatalog<Item093>, Catalog093>();
        services.AddScoped<ICatalog<Item094>, Catalog094>();
        services.AddScoped<ICatalog<Item095>, Catalog095>();
        services.AddScoped<ICatalog<Item096>, Catalog096>();
        services.AddScoped<ICatalog<Item097>, Catalog097>();
        services.AddScoped<ICatalog<Item098>, Catalog098>();
        services.AddScoped<ICatalog<Item099>, Catalog099>();
        return services;
    }

    internal static IServiceCollection Scan() =>
        new ServiceCollection().Scan(scan =>
            scan.FromAssemblyOf<Catalog000>()
                .AddClasses(classes => classes.AssignableTo(typeof(ICatalog<>)))
                .AsImplementedInterfaces()
                .WithScopedLifetime()
        );
}

public interface ICatalog<T>;

public abstract class CatalogBase<T> : ICatalog<T>;

public sealed class Item000;

public sealed class Catalog000 : CatalogBase<Item000>
{
    public Catalog000() { }
}

public sealed class Item001;

public sealed class Catalog001 : CatalogBase<Item001>
{
    public Catalog001() { }
}

public sealed class Item002;

public sealed class Catalog002 : CatalogBase<Item002>
{
    public Catalog002() { }
}

public sealed class Item003;

public sealed class Catalog003 : CatalogBase<Item003>
{
    public Catalog003() { }
}

public sealed class Item004;

public sealed class Catalog004 : CatalogBase<Item004>
{
    public Catalog004() { }
}

public sealed class Item005;

public sealed class Catalog005 : CatalogBase<Item005>
{
    public Catalog005() { }
}

public sealed class Item006;

public sealed class Catalog006 : CatalogBase<Item006>
{
    public Catalog006() { }
}

public sealed class Item007;

public sealed class Catalog007 : CatalogBase<Item007>
{
    public Catalog007() { }
}

public sealed class Item008;

public sealed class Catalog008 : CatalogBase<Item008>
{
    public Catalog008() { }
}

public sealed class Item009;

public sealed class Catalog009 : CatalogBase<Item009>
{
    public Catalog009() { }
}

public sealed class Item010;

public sealed class Catalog010 : CatalogBase<Item010>
{
    public Catalog010() { }
}

public sealed class Item011;

public sealed class Catalog011 : CatalogBase<Item011>
{
    public Catalog011() { }
}

public sealed class Item012;

public sealed class Catalog012 : CatalogBase<Item012>
{
    public Catalog012() { }
}

public sealed class Item013;

public sealed class Catalog013 : CatalogBase<Item013>
{
    public Catalog013() { }
}

public sealed class Item014;

public sealed class Catalog014 : CatalogBase<Item014>
{
    public Catalog014() { }
}

public sealed class Item015;

public sealed class Catalog015 : CatalogBase<Item015>
{
    public Catalog015() { }
}

public sealed class Item016;

public sealed class Catalog016 : CatalogBase<Item016>
{
    public Catalog016() { }
}

public sealed class Item017;

public sealed class Catalog017 : CatalogBase<Item017>
{
    public Catalog017() { }
}

public sealed class Item018;

public sealed class Catalog018 : CatalogBase<Item018>
{
    public Catalog018() { }
}

public sealed class Item019;

public sealed class Catalog019 : CatalogBase<Item019>
{
    public Catalog019() { }
}

public sealed class Item020;

public sealed class Catalog020 : CatalogBase<Item020>
{
    public Catalog020() { }
}

public sealed class Item021;

public sealed class Catalog021 : CatalogBase<Item021>
{
    public Catalog021() { }
}

public sealed class Item022;

public sealed class Catalog022 : CatalogBase<Item022>
{
    public Catalog022() { }
}

public sealed class Item023;

public sealed class Catalog023 : CatalogBase<Item023>
{
    public Catalog023() { }
}

public sealed class Item024;

public sealed class Catalog024 : CatalogBase<Item024>
{
    public Catalog024() { }
}

public sealed class Item025;

public sealed class Catalog025 : CatalogBase<Item025>
{
    public Catalog025() { }
}

public sealed class Item026;

public sealed class Catalog026 : CatalogBase<Item026>
{
    public Catalog026() { }
}

public sealed class Item027;

public sealed class Catalog027 : CatalogBase<Item027>
{
    public Catalog027() { }
}

public sealed class Item028;

public sealed class Catalog028 : CatalogBase<Item028>
{
    public Catalog028() { }
}

public sealed class Item029;

public sealed class Catalog029 : CatalogBase<Item029>
{
    public Catalog029() { }
}

public sealed class Item030;

public sealed class Catalog030 : CatalogBase<Item030>
{
    public Catalog030() { }
}

public sealed class Item031;

public sealed class Catalog031 : CatalogBase<Item031>
{
    public Catalog031() { }
}

public sealed class Item032;

public sealed class Catalog032 : CatalogBase<Item032>
{
    public Catalog032() { }
}

public sealed class Item033;

public sealed class Catalog033 : CatalogBase<Item033>
{
    public Catalog033() { }
}

public sealed class Item034;

public sealed class Catalog034 : CatalogBase<Item034>
{
    public Catalog034() { }
}

public sealed class Item035;

public sealed class Catalog035 : CatalogBase<Item035>
{
    public Catalog035() { }
}

public sealed class Item036;

public sealed class Catalog036 : CatalogBase<Item036>
{
    public Catalog036() { }
}

public sealed class Item037;

public sealed class Catalog037 : CatalogBase<Item037>
{
    public Catalog037() { }
}

public sealed class Item038;

public sealed class Catalog038 : CatalogBase<Item038>
{
    public Catalog038() { }
}

public sealed class Item039;

public sealed class Catalog039 : CatalogBase<Item039>
{
    public Catalog039() { }
}

public sealed class Item040;

public sealed class Catalog040 : CatalogBase<Item040>
{
    public Catalog040() { }
}

public sealed class Item041;

public sealed class Catalog041 : CatalogBase<Item041>
{
    public Catalog041() { }
}

public sealed class Item042;

public sealed class Catalog042 : CatalogBase<Item042>
{
    public Catalog042() { }
}

public sealed class Item043;

public sealed class Catalog043 : CatalogBase<Item043>
{
    public Catalog043() { }
}

public sealed class Item044;

public sealed class Catalog044 : CatalogBase<Item044>
{
    public Catalog044() { }
}

public sealed class Item045;

public sealed class Catalog045 : CatalogBase<Item045>
{
    public Catalog045() { }
}

public sealed class Item046;

public sealed class Catalog046 : CatalogBase<Item046>
{
    public Catalog046() { }
}

public sealed class Item047;

public sealed class Catalog047 : CatalogBase<Item047>
{
    public Catalog047() { }
}

public sealed class Item048;

public sealed class Catalog048 : CatalogBase<Item048>
{
    public Catalog048() { }
}

public sealed class Item049;

public sealed class Catalog049 : CatalogBase<Item049>
{
    public Catalog049() { }
}

public sealed class Item050;

public sealed class Catalog050 : CatalogBase<Item050>
{
    public Catalog050() { }
}

public sealed class Item051;

public sealed class Catalog051 : CatalogBase<Item051>
{
    public Catalog051() { }
}

public sealed class Item052;

public sealed class Catalog052 : CatalogBase<Item052>
{
    public Catalog052() { }
}

public sealed class Item053;

public sealed class Catalog053 : CatalogBase<Item053>
{
    public Catalog053() { }
}

public sealed class Item054;

public sealed class Catalog054 : CatalogBase<Item054>
{
    public Catalog054() { }
}

public sealed class Item055;

public sealed class Catalog055 : CatalogBase<Item055>
{
    public Catalog055() { }
}

public sealed class Item056;

public sealed class Catalog056 : CatalogBase<Item056>
{
    public Catalog056() { }
}

public sealed class Item057;

public sealed class Catalog057 : CatalogBase<Item057>
{
    public Catalog057() { }
}

public sealed class Item058;

public sealed class Catalog058 : CatalogBase<Item058>
{
    public Catalog058() { }
}

public sealed class Item059;

public sealed class Catalog059 : CatalogBase<Item059>
{
    public Catalog059() { }
}

public sealed class Item060;

public sealed class Catalog060 : CatalogBase<Item060>
{
    public Catalog060() { }
}

public sealed class Item061;

public sealed class Catalog061 : CatalogBase<Item061>
{
    public Catalog061() { }
}

public sealed class Item062;

public sealed class Catalog062 : CatalogBase<Item062>
{
    public Catalog062() { }
}

public sealed class Item063;

public sealed class Catalog063 : CatalogBase<Item063>
{
    public Catalog063() { }
}

public sealed class Item064;

public sealed class Catalog064 : CatalogBase<Item064>
{
    public Catalog064() { }
}

public sealed class Item065;

public sealed class Catalog065 : CatalogBase<Item065>
{
    public Catalog065() { }
}

public sealed class Item066;

public sealed class Catalog066 : CatalogBase<Item066>
{
    public Catalog066() { }
}

public sealed class Item067;

public sealed class Catalog067 : CatalogBase<Item067>
{
    public Catalog067() { }
}

public sealed class Item068;

public sealed class Catalog068 : CatalogBase<Item068>
{
    public Catalog068() { }
}

public sealed class Item069;

public sealed class Catalog069 : CatalogBase<Item069>
{
    public Catalog069() { }
}

public sealed class Item070;

public sealed class Catalog070 : CatalogBase<Item070>
{
    public Catalog070() { }
}

public sealed class Item071;

public sealed class Catalog071 : CatalogBase<Item071>
{
    public Catalog071() { }
}

public sealed class Item072;

public sealed class Catalog072 : CatalogBase<Item072>
{
    public Catalog072() { }
}

public sealed class Item073;

public sealed class Catalog073 : CatalogBase<Item073>
{
    public Catalog073() { }
}

public sealed class Item074;

public sealed class Catalog074 : CatalogBase<Item074>
{
    public Catalog074() { }
}

public sealed class Item075;

public sealed class Catalog075 : CatalogBase<Item075>
{
    public Catalog075() { }
}

public sealed class Item076;

public sealed class Catalog076 : CatalogBase<Item076>
{
    public Catalog076() { }
}

public sealed class Item077;

public sealed class Catalog077 : CatalogBase<Item077>
{
    public Catalog077() { }
}

public sealed class Item078;

public sealed class Catalog078 : CatalogBase<Item078>
{
    public Catalog078() { }
}

public sealed class Item079;

public sealed class Catalog079 : CatalogBase<Item079>
{
    public Catalog079() { }
}

public sealed class Item080;

public sealed class Catalog080 : CatalogBase<Item080>
{
    public Catalog080() { }
}

public sealed class Item081;

public sealed class Catalog081 : CatalogBase<Item081>
{
    public Catalog081() { }
}

public sealed class Item082;

public sealed class Catalog082 : CatalogBase<Item082>
{
    public Catalog082() { }
}

public sealed class Item083;

public sealed class Catalog083 : CatalogBase<Item083>
{
    public Catalog083() { }
}

public sealed class Item084;

public sealed class Catalog084 : CatalogBase<Item084>
{
    public Catalog084() { }
}

public sealed class Item085;

public sealed class Catalog085 : CatalogBase<Item085>
{
    public Catalog085() { }
}

public sealed class Item086;

public sealed class Catalog086 : CatalogBase<Item086>
{
    public Catalog086() { }
}

public sealed class Item087;

public sealed class Catalog087 : CatalogBase<Item087>
{
    public Catalog087() { }
}

public sealed class Item088;

public sealed class Catalog088 : CatalogBase<Item088>
{
    public Catalog088() { }
}

public sealed class Item089;

public sealed class Catalog089 : CatalogBase<Item089>
{
    public Catalog089() { }
}

public sealed class Item090;

public sealed class Catalog090 : CatalogBase<Item090>
{
    public Catalog090() { }
}

public sealed class Item091;

public sealed class Catalog091 : CatalogBase<Item091>
{
    public Catalog091() { }
}

public sealed class Item092;

public sealed class Catalog092 : CatalogBase<Item092>
{
    public Catalog092() { }
}

public sealed class Item093;

public sealed class Catalog093 : CatalogBase<Item093>
{
    public Catalog093() { }
}

public sealed class Item094;

public sealed class Catalog094 : CatalogBase<Item094>
{
    public Catalog094() { }
}

public sealed class Item095;

public sealed class Catalog095 : CatalogBase<Item095>
{
    public Catalog095() { }
}

public sealed class Item096;

public sealed class Catalog096 : CatalogBase<Item096>
{
    public Catalog096() { }
}

public sealed class Item097;

public sealed class Catalog097 : CatalogBase<Item097>
{
    public Catalog097() { }
}

public sealed class Item098;

public sealed class Catalog098 : CatalogBase<Item098>
{
    public Catalog098() { }
}

public sealed class Item099;

public sealed class Catalog099 : CatalogBase<Item099>
{
    public Catalog099() { }
}
