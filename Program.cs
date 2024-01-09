// See https://aka.ms/new-console-template for more information
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;

internal class Program
{
    private static void Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;
        TrySetPriorityClass();

        var ok = ParseArgs(args, out string? filePath, out bool debug, out bool useNewLines);
        if (!ok)
            return;

        if (debug)
            RunWithDebugInfo(filePath!, useNewLines);
        else
            RunWithoutDebugInfo(filePath!);
    }

    private static void RunWithoutDebugInfo(string path)
    {
        var runners = ReadUnsafe(path, out _);
        var result = GetResult(runners);
        DumpResult(result, false);
    }

    private static List<Runner> ReadUnsafe(string path, out TimeSpan creationTook)
    {
        var sw = Stopwatch.StartNew();
        using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
        {
            var runners = GetRunners(mmf);
            creationTook = sw.Elapsed;
            foreach (var runner in runners)
            {
                runner.Join();
            }

            return runners;
        }
    }

    private static List<Runner> GetRunners(MemoryMappedFile mmf)
    {
        long offset = 0;
        MemoryMappedViewAccessor accessor;
        MemoryMappedViewAccessor? prevAccessor;
        int length;
        bool last;
        var tasks = new List<Runner>();
        var flag = new ManualResetEvent(false);
        do
        {
            (accessor, prevAccessor, length, last) = GetAccessor(mmf, ref offset);
            //Console.WriteLine("Creating runner: {0}", tasks.Count);
            tasks.Add(new Runner(accessor, prevAccessor, length, flag));
        }
        while (!last);
        flag.Set();
        return tasks;
    }

    private static (MemoryMappedViewAccessor accessor, MemoryMappedViewAccessor? prevAccessor, int length, bool last) GetAccessor(MemoryMappedFile mmf, ref long offset)
    {
        MemoryMappedViewAccessor? prevAccessor = offset > 0 ? mmf.CreateViewAccessor(offset - Consts.PrevAccessorLength, Consts.PrevAccessorLength, MemoryMappedFileAccess.Read) : null;
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

    private static void DumpResult(Dictionary<int, CityInfo> result, bool useNewLines)
    {
        string separator = useNewLines ? ",\n" : ", ";
        var prev = "{";
        foreach (var item in result.OrderBy(x => x.Value.City))
        {
            item.Value.PrintResult(prev);
            prev = separator;
        }
        Console.WriteLine("}");
    }

    private static bool ParseArgs(string[] args, [NotNullWhen(true)] out string? filePath, out bool debug, out bool useNewLines)
    {
        filePath = default;
        debug = default;
        useNewLines = default;
        if (args.Length == 0)
        {
            Console.WriteLine("Path to measurements file was not provided");
            return false;
        }

        filePath = args[0];
        if (!File.Exists(filePath))
        {
            Console.WriteLine($"File: {filePath} doesn't exists (full path: {Path.GetFullPath(filePath)})");
            return false;
        }
        var restArgs = args.Skip(1).ToArray();
        if (args.Length > 1)
        {
            debug = restArgs.Any(x => x.IndexOf("debug", StringComparison.InvariantCultureIgnoreCase) >= 0);
            useNewLines = restArgs.Any(x => x.IndexOf("nl", StringComparison.InvariantCultureIgnoreCase) >= 0);
        }
        return true;
    }

    private static void RunWithDebugInfo(string path, bool useNewLines)
    {
        Console.WriteLine($"Start: {Sse2.IsSupported}; {Vector512.IsHardwareAccelerated}; {Vector256.IsHardwareAccelerated}!");
        Console.WriteLine();

        using var proc = Process.GetCurrentProcess();
        var sw = Stopwatch.StartNew();
        var runners = ReadUnsafe(path, out var creation);
        var result = GetResult(runners);
        sw.Stop();
        DumpResult(result, useNewLines);
        double peak = proc.PeakWorkingSet64;
        peak = peak / (1024 * 1024);
        Console.WriteLine();
        var sum = runners.Select(x => x.Result).Sum();
        Console.WriteLine($"Finished: {sw.Elapsed.TotalSeconds:0.###} sec; creating threads took: {creation.TotalMilliseconds:0.###} ms; peak: {peak:0.###} MB; lines: {sum}; cities: {result.Count}; runners: {runners.Count}");
    }

    private static void TrySetPriorityClass()
    {
        try
        {
            using var proc = Process.GetCurrentProcess();
            proc.PriorityClass = ProcessPriorityClass.AboveNormal;
        }
        catch { }
    }
}
