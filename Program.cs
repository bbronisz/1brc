// See https://aka.ms/new-console-template for more information
using System.Diagnostics;
using System.IO.MemoryMappedFiles;

const string path = @"C:\Users\bbr\Downloads\measurements.txt";

await ReadUnsafe(path);

async Task ReadUnsafe(string path)
{
    Console.WriteLine("Start!");
    var sw = Stopwatch.StartNew();
    using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
    {
        var tasks = GetTasks(mmf);
        await Task.WhenAll(tasks.Select(x => x.Awaitable));
        Console.WriteLine("Sum: {0}", tasks.Select(x => x.Result).Sum());
    }
    Console.WriteLine("Finished: {0:0.###} sec", sw.Elapsed.TotalSeconds);
}

List<Runner> GetTasks(MemoryMappedFile mmf)
{
    long offset = 0;
    (MemoryMappedViewAccessor accessor, bool last) accessorPair;
    var tasks = new List<Runner>();
    do
    {
        accessorPair = TryGetAccessor(mmf, ref offset);
        //Console.WriteLine("Creating runner: {0}", tasks.Count);
        tasks.Add(new Runner(accessorPair.accessor));
    }
    while (!accessorPair.last);
    return tasks;
}

(MemoryMappedViewAccessor, bool) TryGetAccessor(MemoryMappedFile mmf, ref long offset)
{
    try
    {
        var accessor = mmf.CreateViewAccessor(offset, Consts.QuarterOfGB, MemoryMappedFileAccess.Read);
        offset += Consts.QuarterOfGB;
        return (accessor, false);
    }
    catch (Exception)
    {
        return (mmf.CreateViewAccessor(offset, 0), true);
        //Console.WriteLine("Error: {0}", ex);
    }
}

class Runner
{
    private readonly MemoryMappedViewAccessor accessor;
    public Runner(MemoryMappedViewAccessor accessor)
    {
        this.accessor = accessor;
        Awaitable = Task.Run(() => CountLines());
    }

    public Task Awaitable { get; }
    public int Result { get; private set; }

    unsafe void CountLines()
    {
        if (accessor == null)
        {
            Console.WriteLine("Accessor is null");
            return;
        }
        int count = 0;
        using (accessor)
        {
            byte* buffor = null;
            long i = 0;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref buffor);
            try
            {
                while (i < Consts.QuarterOfGB && *buffor != 0)
                {
                    if ((char)*buffor == '\n')
                    {
                        count++;
                    }
                    buffor++;
                    i++;
                }
            }
            finally
            {
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
        Result = count;
    }
}


