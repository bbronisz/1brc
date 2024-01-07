// See https://aka.ms/new-console-template for more information
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;

const string path = @"C:\Users\bbr\Downloads\measurements-big.txt";//measurements-big.txt";

await ReadUnsafe(path);

async Task ReadUnsafe(string path)
{
    Console.OutputEncoding = Encoding.UTF8;
    Console.WriteLine("Start Wrocław Suwałki!");
    using var proc = Process.GetCurrentProcess();
    var sw = Stopwatch.StartNew();
    using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
    {
        var tasks = GetTasks(mmf);
        await Task.WhenAll(tasks.Select(x => x.Awaitable));
        Console.WriteLine();
        Console.WriteLine("Sum: {0}", tasks.Select(x => (long)x.Result).Sum());
        var result = tasks.First().Results;
        foreach (var task in tasks.Skip(1))
            foreach (var kv in task.Results)
            {
                if (result.TryGetValue(kv.Key, out CityInfo? cityInfo))
                {
                    cityInfo.Merge(kv.Value);
                    continue;
                }
                result.Add(kv.Key, kv.Value);
            }
        Console.Write("{");
        foreach (var item in result.OrderBy(x => x.Value.City))
        {
            item.Value.PrintResult();
        }
        Console.WriteLine("}");
    }
    double peak = proc.PeakWorkingSet64;
    peak = peak / (1024 * 1024);
    Console.WriteLine("Finished: {0:0.###} sec; peak: {1:0.###} MB", sw.Elapsed.TotalSeconds, peak);
}

unsafe void ReadText(MemoryMappedFile mmf)
{
    using (var acc = mmf.CreateViewAccessor(0, 14000, MemoryMappedFileAccess.Read))
    {
        byte* bytes = null;
        acc.SafeMemoryMappedViewHandle.AcquirePointer(ref bytes);
        var span = new ReadOnlySpan<byte>(bytes, 14000);
        Console.WriteLine(Encoding.UTF8.GetString(span));
        acc.SafeMemoryMappedViewHandle.ReleasePointer();
    }
}

List<Runner> GetTasks(MemoryMappedFile mmf)
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

(MemoryMappedViewAccessor accessor, MemoryMappedViewAccessor? prevAccessor, int length, bool last) GetAccessor(MemoryMappedFile mmf, ref long offset)
{
    var prevAccessor = offset > 0 ? mmf.CreateViewAccessor(offset - 100, 100, MemoryMappedFileAccess.Read) : null;
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


class Runner
{
    private readonly MemoryMappedViewAccessor accessor;
    private readonly MemoryMappedViewAccessor? prevAccessor;
    private readonly Dictionary<int, CityInfo> results;
    private readonly int length;
    public Runner(MemoryMappedViewAccessor accessor, MemoryMappedViewAccessor? prevAccessor, int length)
    {
        this.accessor = accessor;
        this.prevAccessor = prevAccessor;
        if (length > 0)
            this.length = length;
        else
        {
            this.length = (int)accessor.SafeMemoryMappedViewHandle.ByteLength;
            //Console.WriteLine("Byte length: {0}", this.length);
        }
        results = new Dictionary<int, CityInfo>(512);
        Awaitable = Task.Run(() => CountLines());
    }

    public Task Awaitable { get; }
    public int Result { get; private set; }
    public Dictionary<int, CityInfo> Results => results;

    unsafe void CountLines()
    {
        int count = 0;
        using (accessor)
        {
            byte* buffor = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref buffor);
            try
            {
                var byteSpan = new ReadOnlySpan<byte>(buffor, length);
                var index = byteSpan.IndexOf((byte)'\n');
                count++;
                byteSpan = byteSpan.Slice(index + 1);
                index = byteSpan.IndexOf((byte)'\n');

                while (index >= 0)
                {
                    count++;
                    var separatorIndex = byteSpan.IndexOf((byte)';');
                    var citySpan = byteSpan.Slice(0, separatorIndex);
                    var hc = new HashCode();
                    hc.AddBytes(citySpan);
                    var hashCode = hc.ToHashCode();
                    var value = double.Parse(byteSpan.Slice(separatorIndex + 1, index - separatorIndex - 1));
                    if (results.TryGetValue(hashCode, out CityInfo? cityInfo))
                        cityInfo.Add(value);
                    else
                        results.Add(hashCode, new CityInfo(Encoding.UTF8.GetString(citySpan), value));
                    if (byteSpan.Length <= index + 1)
                        break;
                    byteSpan = byteSpan.Slice(index + 1);
                    index = byteSpan.IndexOf((byte)'\n');
                }
            }
            finally
            {
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
        //Console.Write('.');
        Result = count;
    }
}

class CityInfo
{
    public CityInfo(string city, double value)
    {
        City = city;
        //HashCode = hashCode;
        Min = Max = Sum = value;
        Count = 1;
    }

    //public int HashCode { get; }
    public string City { get; }
    public double Min { get; private set; }
    public double Max { get; private set; }
    public double Sum { get; private set; }
    public int Count { get; private set; }

    public void Add(double value)
    {
        if (Min > value) Min = value;
        else if (Max < value) Max = value;
        Sum += value;
        Count++;
    }

    public void Merge(CityInfo other)
    {
        if (other.Min < Min) Min = other.Min;
        if (other.Min > Max) Max = other.Max;
        Sum += other.Sum;
        Count += other.Count;
    }

    public void PrintResult()
    {
        Console.Write("{0}={1:0.0}/{2:0.0}/{3:0.0}, ", City, Min, Sum / Count, Max);
    }
}

