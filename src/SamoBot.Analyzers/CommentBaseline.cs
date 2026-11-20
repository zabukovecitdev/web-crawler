using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace SamoBot.Analyzers;

internal sealed class CommentBaseline
{
    public const string FileName = "comments-baseline.txt";

    private readonly string _root;
    private readonly Dictionary<string, int> _allowed;

    private CommentBaseline(string root, Dictionary<string, int> allowed)
    {
        _root = root;
        _allowed = allowed;
    }

    public static CommentBaseline? Find(AnalyzerOptions options)
    {
        var file = options.AdditionalFiles.FirstOrDefault(
            f => string.Equals(Path.GetFileName(f.Path), FileName, StringComparison.OrdinalIgnoreCase));
        return file is null
            ? null
            : new CommentBaseline(Path.GetDirectoryName(file.Path) ?? string.Empty, Parse(file.GetText()?.ToString()));
    }

    public string RelativePath(string path) =>
        Normalize(path.StartsWith(_root, StringComparison.OrdinalIgnoreCase) ? path.Substring(_root.Length) : path);

    public int Allowed(string relativePath) => _allowed.TryGetValue(relativePath, out var count) ? count : 0;

    private static Dictionary<string, int> Parse(string? text)
    {
        var allowed = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var line in (text ?? string.Empty).Split('\n'))
        {
            var parts = line.Trim().Split('\t');
            if (parts.Length == 2 && int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var count))
            {
                allowed[Normalize(parts[0])] = count;
            }
        }

        return allowed;
    }

    private static string Normalize(string path) => path.Replace('\\', '/').TrimStart('/');
}
