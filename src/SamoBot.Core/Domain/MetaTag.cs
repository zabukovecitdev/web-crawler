namespace SamoBot.Core.Domain;

/// <summary>A &lt;meta&gt; tag of a page: its name (or http-equiv, when <see cref="HttpEquiv"/> is true) and its content, as written.</summary>
public sealed record MetaTag(string Name, string Content, bool HttpEquiv = false);
