// See https://aka.ms/new-console-template for more information
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Text;

internal class Program
{
    private static async Task Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        var ok = ParseArgs(args, out bool debug, out string? filePath);
        if (!ok)
            return;

        if (debug)
            await RunWithDebugInfo(filePath!);
        else
            await RunWithoutDebugInfo(filePath!);
    }

    private static async Task RunWithoutDebugInfo(string path)
    {
        var runners = await ReadUnsafe(path);
        var result = GetResult(runners);
        DumpResult(result);
    }

    private static async Task<List<Runner>> ReadUnsafe(string path)
    {
        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var runners = GetTasks(mmf);
            await Task.WhenAll(runners.Select(x => x.Awaitable));
            return runners;
        }
    }

    private static List<Runner> GetTasks(MemoryMappedFile mmf)
    {
        long offset = 0;
        (MemoryMappedViewAccessor accessor, MemoryMappedViewAccessor? prevAccessor, int length, bool last) accessorInfo;
        var tasks = new List<Runner>();
        do
        {
            accessorInfo = GetAccessor(mmf, ref offset);
            //Console.WriteLine("Creating runner: {0}", tasks.Count);
            tasks.Add(new Runner(accessorInfo.accessor, accessorInfo.prevAccessor, accessorInfo.length));
        }
        while (!accessorInfo.last);
        return tasks;
    }

    private static (MemoryMappedViewAccessor accessor, MemoryMappedViewAccessor? prevAccessor, int length, bool last) GetAccessor(MemoryMappedFile mmf, ref long offset)
    {
        var prevAccessor = offset > 0 ? mmf.CreateViewAccessor(offset - Consts.PrevAccessorLength, Consts.PrevAccessorLength, MemoryMappedFileAccess.Read) : null;
        try
        {
            var length = Consts.OneThreadBlockSize;
            var accessor = mmf.CreateViewAccessor(offset, length, MemoryMappedFileAccess.Read);
            offset += Consts.OneThreadBlockSize;
            return (accessor, prevAccessor, length, false);
        }
        catch (Exception)
        {
            return (mmf.CreateViewAccessor(offset, 0, MemoryMappedFileAccess.Read), prevAccessor, 0, true);
            //Console.WriteLine("Error: {0}", ex);
        }
    }

    private static Dictionary<int, CityInfo> GetResult(List<Runner> runners)
    {
        Dictionary<int, CityInfo> result = runners.First().Results;

        foreach (Runner task in runners.Skip(1))
            foreach (KeyValuePair<int, CityInfo> kv in task.Results)
            {
                if (result.TryGetValue(kv.Key, out CityInfo? cityInfo))
                {
                    cityInfo.Merge(kv.Value);
                    continue;
                }
                result.Add(kv.Key, kv.Value);
            }

        return result;
    }

    private static void DumpResult(Dictionary<int, CityInfo> result)
    {
        var prev = "{";
        foreach (var item in result.OrderBy(x => x.Value.City))
        {
            item.Value.PrintResult(prev);
            prev = ", ";
        }
        Console.WriteLine("}");
    }

    private static bool ParseArgs(string[] args, out bool debug, [NotNullWhen(true)] out string? filePath)
    {
        filePath = default;
        debug = default;
        if (args.Length == 0)
        {
            Console.WriteLine("Path to measurements file was not provided");
            return false;
        }
        filePath = args[0];
        if (!File.Exists(filePath))
        {
            Console.WriteLine("File: {0} doesn't exists (full path: {1})", filePath, Path.GetFullPath(filePath));
            return false;
        }
        if (args.Length > 1)
        {
            var secondArg = args[1];
            debug = secondArg.Length > 0 && secondArg.IndexOf("debug", StringComparison.InvariantCultureIgnoreCase) >= 0;
        }
        return true;
    }

    private static async Task RunWithDebugInfo(string path)
    {
        Console.WriteLine("Start!");
        using var proc = Process.GetCurrentProcess();
        var sw = Stopwatch.StartNew();
        var sw2 = new Stopwatch();
        var runners = await ReadUnsafe(path);
        Console.WriteLine();
        Console.WriteLine("Sum: {0}", runners.Select(x => x.Result).Sum());
        sw2.Start();
        var result = GetResult(runners);
        sw2.Stop();
        DumpResult(result);
        double peak = proc.PeakWorkingSet64;
        peak = peak / (1024 * 1024);
        Console.WriteLine("Finished: {0:0.###} sec; merging took: {2:0.###} ms; peak: {1:0.###} MB", sw.Elapsed.TotalSeconds, peak, sw2.Elapsed.TotalMilliseconds);
    }
}
