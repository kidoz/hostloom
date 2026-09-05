using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace HostLoom.Composition.Generators;

internal sealed partial class DeclarationCompiler
{
    private readonly GeneratorAttributeSyntaxContext _context;
    private readonly CancellationToken _cancellation;
    private readonly SemanticModel _model;
    private readonly IMethodSymbol _method;
    private readonly MethodDeclarationSyntax _syntax;
    private readonly List<Diagnostic> _diagnostics = [];
    private readonly List<Registration> _registrations = [];
    private readonly HashSet<string> _groups = new(StringComparer.Ordinal);
    private int _ruleNumber;

    internal DeclarationCompiler(
        GeneratorAttributeSyntaxContext context,
        CancellationToken cancellation
    )
    {
        _context = context;
        _cancellation = cancellation;
        _model = context.SemanticModel;
        _method = (IMethodSymbol)context.TargetSymbol;
        _syntax = (MethodDeclarationSyntax)context.TargetNode;
    }

    internal GenerationResult Compile()
    {
        _cancellation.ThrowIfCancellationRequested();
        IMethodSymbol? factory = ValidateDeclaration();
        if (factory is not null)
        {
            ParseStatements(_syntax.Body!.Statements, _method.Parameters[0], null);
            ValidateConflicts();
        }
        GeneratedFile? output =
            factory is not null && _diagnostics.Count == 0 ? Emit(factory) : null;
        return new GenerationResult(output, _diagnostics.ToImmutableArray());
    }

    private IMethodSymbol? ValidateDeclaration()
    {
        if (
            !_method.IsStatic
            || !_method.ReturnsVoid
            || _method.IsGenericMethod
            || _method.IsAsync
            || _method.Parameters.Length != 1
            || _method.Parameters[0].RefKind != RefKind.None
            || !IsType(_method.Parameters[0].Type, CompositionGenerator.BuilderName)
            || _syntax.Body is null
            || _syntax.Modifiers.Any(SyntaxKind.PartialKeyword)
        )
        {
            Error(
                CompositionDiagnostics.Declaration,
                _syntax,
                "Declare a non-generic static void method with one CompositionRuleBuilder parameter and a block body."
            );
            return null;
        }
        for (
            INamedTypeSymbol? type = _method.ContainingType;
            type is not null;
            type = type.ContainingType
        )
        {
            if (
                type.TypeKind != TypeKind.Class
                || type.IsRecord
                || type.IsFileLocal
                || type.Arity != 0
                || type.DeclaringSyntaxReferences.Any(reference =>
                    reference.GetSyntax(_cancellation) is not ClassDeclarationSyntax declaration
                    || !declaration.Modifiers.Any(SyntaxKind.PartialKeyword)
                )
            )
            {
                Error(
                    CompositionDiagnostics.Declaration,
                    _syntax,
                    "The declaration and every containing type must be non-generic, non-file-local partial classes."
                );
                return null;
            }
        }
        AttributeData attribute = _context.Attributes[0];
        string? name =
            attribute.ConstructorArguments.Length == 1
                ? attribute.ConstructorArguments[0].Value as string
                : null;
        IMethodSymbol[] factories = name is null
            ? []
            : _method.ContainingType.GetMembers(name).OfType<IMethodSymbol>().ToArray();
        if (
            factories.Length != 1
            || !factories[0].IsStatic
            || factories[0].IsGenericMethod
            || factories[0].Parameters.Length != 0
            || !factories[0].IsPartialDefinition
            || factories[0].PartialImplementationPart is not null
            || !IsType(factories[0].ReturnType, CompositionGenerator.PlanName)
            || factories[0].ReturnsByRef
            || factories[0].ReturnsByRefReadonly
        )
        {
            Error(
                CompositionDiagnostics.Declaration,
                _syntax,
                "Name one unimplemented, parameterless static partial CompositionPlan factory in the same type."
            );
            return null;
        }
        foreach (IMethodSymbol other in _method.ContainingType.GetMembers().OfType<IMethodSymbol>())
        {
            if (SymbolEqualityComparer.Default.Equals(other, _method))
                continue;
            if (
                other
                    .GetAttributes()
                    .Any(item =>
                        IsType(item.AttributeClass, CompositionGenerator.AttributeName)
                        && item.ConstructorArguments.Length == 1
                        && string.Equals(
                            item.ConstructorArguments[0].Value as string,
                            name,
                            StringComparison.Ordinal
                        )
                    )
            )
            {
                Error(
                    CompositionDiagnostics.Declaration,
                    _syntax,
                    $"Factory '{name}' is claimed by both '{_method.Name}' and '{other.Name}'.",
                    other.Locations.FirstOrDefault()
                );
            }
        }
        return factories[0];
    }

    private void ParseStatements(
        SyntaxList<StatementSyntax> statements,
        ISymbol receiver,
        string? group
    )
    {
        foreach (StatementSyntax statement in statements)
        {
            _cancellation.ThrowIfCancellationRequested();
            if (
                statement
                is not ExpressionStatementSyntax
                {
                    Expression: InvocationExpressionSyntax invocation
                }
            )
            {
                Error(
                    CompositionDiagnostics.Declaration,
                    statement,
                    "Only chained rule calls and inline Group calls are supported; helpers, locals and control flow are not evaluated."
                );
                continue;
            }
            IMethodSymbol? target = Method(invocation);
            if (
                target?.Name == "Group"
                && IsType(target.ContainingType, CompositionGenerator.BuilderName)
            )
            {
                ParseGroup(invocation, receiver, group);
            }
            else
            {
                ParseRule(invocation, receiver, group);
            }
        }
    }

    private void ParseGroup(InvocationExpressionSyntax invocation, ISymbol receiver, string? parent)
    {
        if (
            parent is not null
            || !HasReceiver(invocation, receiver)
            || invocation.ArgumentList.Arguments.Count != 2
            || invocation.ArgumentList.Arguments.Any(static argument =>
                argument.NameColon is not null
            )
            || _model
                .GetConstantValue(invocation.ArgumentList.Arguments[0].Expression, _cancellation)
                .Value
                is not string name
            || string.IsNullOrWhiteSpace(name)
        )
        {
            Error(
                CompositionDiagnostics.Declaration,
                invocation,
                "Group requires a unique constant name and an inline lambda; nested groups are unsupported."
            );
            return;
        }
        ExpressionSyntax expression = invocation.ArgumentList.Arguments[1].Expression;
        ParameterSyntax? parameter = expression switch
        {
            SimpleLambdaExpressionSyntax lambda => lambda.Parameter,
            ParenthesizedLambdaExpressionSyntax lambda
                when lambda.ParameterList.Parameters.Count == 1 => lambda.ParameterList.Parameters[
                0
            ],
            _ => null,
        };
        if (
            parameter is null
            || expression is not LambdaExpressionSyntax body
            || body.AsyncKeyword != default
            || _model.GetDeclaredSymbol(parameter, _cancellation) is not IParameterSymbol symbol
            || !_groups.Add(name)
        )
        {
            Error(
                CompositionDiagnostics.Declaration,
                invocation,
                "Group requires a unique name and a synchronous inline lambda with one builder parameter."
            );
            return;
        }
        if (body.Body is BlockSyntax block)
        {
            ParseStatements(block.Statements, symbol, name);
        }
        else if (body.Body is InvocationExpressionSyntax call)
        {
            ParseRule(call, symbol, name);
        }
        else
            Error(
                CompositionDiagnostics.Declaration,
                body,
                "The group body must contain rule calls."
            );
    }

    private void ParseRule(InvocationExpressionSyntax invocation, ISymbol receiver, string? group)
    {
        int errorsBefore = _diagnostics.Count;
        var chain = new List<InvocationExpressionSyntax>();
        InvocationExpressionSyntax current = invocation;
        while (true)
        {
            chain.Add(current);
            if (
                current.Expression is MemberAccessExpressionSyntax
                {
                    Expression: InvocationExpressionSyntax inner
                }
            )
                current = inner;
            else
                break;
        }
        chain.Reverse();
        var rule = new Rule(++_ruleNumber, invocation, group);
        IMethodSymbol? root = Method(chain[0]);
        if (
            root is null
            || !IsType(root.ContainingType, CompositionGenerator.BuilderName)
            || !HasReceiver(chain[0], receiver)
            || (root.Name != "AddTypes" && root.Name != "AddClasses")
        )
        {
            Error(
                CompositionDiagnostics.Declaration,
                chain[0],
                "Start each rule with this declaration's builder.AddTypes or builder.AddClasses."
            );
            return;
        }
        if (!Positional(chain[0]))
            return;
        rule.Discover = root.Name == "AddClasses";
        if (!rule.Discover)
            rule.Types.AddRange(ReadTypes(chain[0], root, allowEmpty: true));
        foreach (InvocationExpressionSyntax call in chain.Skip(1))
        {
            _cancellation.ThrowIfCancellationRequested();
            IMethodSymbol? method = Method(call);
            if (
                method is null
                || !IsType(method.ContainingType, CompositionGenerator.TypeBuilderName)
                || !Positional(call)
            )
            {
                Error(
                    CompositionDiagnostics.Declaration,
                    call,
                    "Only documented composition rule methods are supported."
                );
                continue;
            }
            switch (method.Name)
            {
                case "AssignableTo":
                case "AssignableToAny":
                    rule.Selectors.Add(ReadTypes(call, method));
                    break;
                case "AsSelf":
                case "AsImplementedInterfaces":
                case "As":
                    if (rule.Projection is not null)
                        Error(
                            CompositionDiagnostics.Projection,
                            call,
                            "Specify one service projection per rule."
                        );
                    rule.Projection = method.Name;
                    if (method.Name == "As")
                        rule.Services.AddRange(ReadTypes(call, method));
                    break;
                case "WithTransientLifetime":
                    SetLifetime(rule, "Transient", call);
                    break;
                case "WithScopedLifetime":
                    SetLifetime(rule, "Scoped", call);
                    break;
                case "WithSingletonLifetime":
                    SetLifetime(rule, "Singleton", call);
                    break;
                case "WithLifetime":
                    object? value = _model
                        .GetConstantValue(call.ArgumentList.Arguments[0].Expression, _cancellation)
                        .Value;
                    string? lifetime = value is int number
                        ? number switch
                        {
                            0 => "Singleton",
                            1 => "Scoped",
                            2 => "Transient",
                            _ => null,
                        }
                        : null;
                    if (lifetime is null)
                        Error(
                            CompositionDiagnostics.Policy,
                            call,
                            "WithLifetime requires a valid compile-time ServiceLifetime constant."
                        );
                    else
                        SetLifetime(rule, lifetime, call);
                    break;
                case "ExpectOne":
                case "ExpectMany":
                    if (rule.Cardinality is not null)
                        Error(
                            CompositionDiagnostics.Policy,
                            call,
                            "Specify cardinality exactly once."
                        );
                    rule.Cardinality = method.Name == "ExpectOne" ? "One" : "Many";
                    break;
                case "AllowEmpty":
                    rule.AllowEmpty = true;
                    break;
                default:
                    Error(
                        CompositionDiagnostics.Declaration,
                        call,
                        $"Rule method '{method.Name}' is not supported."
                    );
                    break;
            }
        }
        if (rule.Lifetime is null || rule.Cardinality is null)
            Error(
                CompositionDiagnostics.Policy,
                invocation,
                "Each rule requires an explicit lifetime and ExpectOne or ExpectMany."
            );
        if (rule.Projection is null)
            Error(
                CompositionDiagnostics.Projection,
                invocation,
                "Each rule requires AsSelf, AsImplementedInterfaces or an explicit As service projection."
            );
        if (rule.Discover && rule.Selectors.Count == 0)
            Error(
                CompositionDiagnostics.Selection,
                invocation,
                "AddClasses requires an assignability selector to bound discovery."
            );
        if (_diagnostics.Count == errorsBefore)
            Select(rule);
    }

    private void SetLifetime(Rule rule, string lifetime, SyntaxNode call)
    {
        if (rule.Lifetime is not null)
            Error(CompositionDiagnostics.Policy, call, "Specify lifetime exactly once.");
        rule.Lifetime = lifetime;
    }

    private List<INamedTypeSymbol> ReadTypes(
        InvocationExpressionSyntax call,
        IMethodSymbol method,
        bool allowEmpty = false
    )
    {
        var types = new List<INamedTypeSymbol>();
        if (method.IsGenericMethod)
        {
            foreach (ITypeSymbol type in method.TypeArguments)
            {
                if (
                    type is INamedTypeSymbol named
                    && named.TypeKind != TypeKind.Error
                    && !ContainsParameters(named)
                )
                    types.Add(named);
                else
                    Error(
                        CompositionDiagnostics.Declaration,
                        call,
                        "Type arguments must name concrete types known to the compilation."
                    );
            }
        }
        else
        {
            foreach (ArgumentSyntax argument in call.ArgumentList.Arguments)
            {
                if (
                    argument.Expression is TypeOfExpressionSyntax typeOf
                    && _model.GetTypeInfo(typeOf.Type, _cancellation).Type is INamedTypeSymbol type
                    && type.TypeKind != TypeKind.Error
                )
                    types.Add(type);
                else
                    Error(
                        CompositionDiagnostics.Declaration,
                        argument,
                        "Use explicit typeof expressions; runtime arrays and helper results are not evaluated."
                    );
            }
        }
        if (types.Count == 0 && !allowEmpty)
            Error(
                CompositionDiagnostics.Selection,
                call,
                "The selector requires at least one type."
            );
        return types;
    }

    private bool IsType(ITypeSymbol? type, string metadataName) =>
        SymbolEqualityComparer.Default.Equals(
            type,
            _model.Compilation.GetTypeByMetadataName(metadataName)
        );

    private IMethodSymbol? Method(InvocationExpressionSyntax call) =>
        _model.GetSymbolInfo(call, _cancellation).Symbol as IMethodSymbol;

    private bool HasReceiver(InvocationExpressionSyntax call, ISymbol receiver) =>
        call.Expression is MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax name }
        && SymbolEqualityComparer.Default.Equals(
            _model.GetSymbolInfo(name, _cancellation).Symbol,
            receiver
        );

    private bool Positional(InvocationExpressionSyntax call)
    {
        if (
            !call.ArgumentList.Arguments.Any(static argument =>
                argument.NameColon is not null || argument.RefKindKeyword != default
            )
        )
            return true;
        Error(
            CompositionDiagnostics.Declaration,
            call,
            "Use positional rule arguments without ref/out modifiers."
        );
        return false;
    }

    private void Error(
        DiagnosticDescriptor descriptor,
        SyntaxNode node,
        string message,
        Location? other = null
    ) =>
        _diagnostics.Add(
            Diagnostic.Create(
                descriptor,
                node.GetLocation(),
                other is null ? null : [other],
                properties: null,
                messageArgs: [$"Rule declaration '{_method.Name}': {message}"]
            )
        );

    private sealed class Rule(int number, InvocationExpressionSyntax syntax, string? group)
    {
        internal int Number { get; } = number;
        internal InvocationExpressionSyntax Syntax { get; } = syntax;
        internal string? Group { get; } = group;
        internal bool Discover { get; set; }
        internal bool AllowEmpty { get; set; }
        internal List<INamedTypeSymbol> Types { get; } = [];
        internal List<List<INamedTypeSymbol>> Selectors { get; } = [];
        internal List<INamedTypeSymbol> Services { get; } = [];
        internal string? Projection { get; set; }
        internal string? Lifetime { get; set; }
        internal string? Cardinality { get; set; }
    }

    private sealed class Registration(
        INamedTypeSymbol implementation,
        INamedTypeSymbol service,
        Rule rule
    )
    {
        internal INamedTypeSymbol Implementation { get; } = implementation;
        internal INamedTypeSymbol Service { get; } = service;
        internal Rule Rule { get; } = rule;
    }
}
