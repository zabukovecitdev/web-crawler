namespace Samobot.Infrastructure.Enums;

public static class IndexJobOperationExtensions
{
    public static string AsString(this IndexJobOperation operation) => operation switch
    {
        IndexJobOperation.Index => "Index",
        IndexJobOperation.Delete => "Delete",
        IndexJobOperation.Reindex => "Reindex",
        _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, null)
    };
}
