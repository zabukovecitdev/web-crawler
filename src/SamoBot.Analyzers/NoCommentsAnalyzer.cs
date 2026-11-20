using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;

namespace SamoBot.Analyzers;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NoCommentsAnalyzer : DiagnosticAnalyzer
{
    public static readonly DiagnosticDescriptor CommentNotAllowed = new(
        "SB0001",
        "Comments are not allowed",
        "Comments are not allowed ({0} in this file, the baseline allows {1}). Record the decision in docs/adr (see ADR 0001).",
        "Maintainability",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor BaselineTooHigh = new(
        "SB0002",
        "The comment baseline is higher than needed",
        "{0} has {1} comments but comments-baseline.txt allows {2}. Lower the baseline to {1}.",
        "Maintainability",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly ImmutableHashSet<SyntaxKind> CommentKinds = ImmutableHashSet.Create(
        SyntaxKind.SingleLineCommentTrivia,
        SyntaxKind.MultiLineCommentTrivia,
        SyntaxKind.SingleLineDocumentationCommentTrivia,
        SyntaxKind.MultiLineDocumentationCommentTrivia);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [CommentNotAllowed, BaselineTooHigh];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxTreeAction(Analyze);
    }

    private static void Analyze(SyntaxTreeAnalysisContext context)
    {
        var baseline = CommentBaseline.Find(context.Options);
        var path = baseline?.RelativePath(context.Tree.FilePath) ?? context.Tree.FilePath;
        var allowed = baseline?.Allowed(path) ?? 0;
        var comments = Comments(context.Tree.GetRoot(context.CancellationToken)).ToList();
        var count = comments.Sum(Weight);

        if (count > allowed)
        {
            ReportComments(context, comments, count, allowed);
        }
        else if (count < allowed)
        {
            var start = Location.Create(context.Tree, new TextSpan(0, 0));
            context.ReportDiagnostic(Diagnostic.Create(BaselineTooHigh, start, path, count, allowed));
        }
    }

    private static void ReportComments(SyntaxTreeAnalysisContext context, List<SyntaxTrivia> comments, int count, int allowed)
    {
        foreach (var comment in comments)
        {
            context.ReportDiagnostic(Diagnostic.Create(CommentNotAllowed, comment.GetLocation(), count, allowed));
        }
    }

    private static IEnumerable<SyntaxTrivia> Comments(SyntaxNode root) =>
        root.DescendantTrivia(descendIntoTrivia: false).Where(trivia => CommentKinds.Contains(trivia.Kind()));

    private static int Weight(SyntaxTrivia comment) =>
        comment.IsKind(SyntaxKind.SingleLineDocumentationCommentTrivia) && comment.GetStructure() is { } structure
            ? structure.DescendantTrivia(descendIntoTrivia: true).Count(t => t.IsKind(SyntaxKind.DocumentationCommentExteriorTrivia))
            : 1;
}
