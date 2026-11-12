using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Indexing;

public sealed class IndexBatchHandler(IIndexQueue queue, ISearchIndex searchIndex) : IWorkHandler
{
    private const int BatchSize = 50;

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var pages = await queue.Claim(BatchSize, ct);
        if (pages.Count == 0)
        {
            return 0;
        }

        await searchIndex.AddOrUpdate(pages, ct);
        await queue.MarkIndexed(pages.Select(page => page.PageId).ToList(), ct);
        return pages.Count;
    }
}
