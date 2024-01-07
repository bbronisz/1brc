// See https://aka.ms/new-console-template for more information
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Text;

const string path = @"C:\Users\bbr\Downloads\measurements-big.txt";

await ReadUnsafe(path);

async Task ReadUnsafe(string path)
{
    Console.WriteLine("Start!");
    using var proc = Process.GetCurrentProcess();
    var sw = Stopwatch.StartNew();
    using (var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open))
    {
        var tasks = GetTasks(mmf);
        await Task.WhenAll(tasks.Select(x => x.Awaitable));
        Console.WriteLine("Sum: {0}", tasks.Select(x => (long)x.Result).Sum());
    }
    double peak = proc.PeakWorkingSet64;
    peak = peak / (1024 * 1024);
    Console.WriteLine("Finished: {0:0.###} sec; peak: {1:0.###} MB", sw.Elapsed.TotalSeconds, peak);
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
    //int goBack = FindLastNewLine(mmf, offset);
    var prevAccessor = offset > 0 ? mmf.CreateViewAccessor(offset - 100, 100, MemoryMappedFileAccess.Read) : null;
    try
    {
        var length = Consts.QuarterOfGB;
        var accessor = mmf.CreateViewAccessor(offset, length, MemoryMappedFileAccess.Read);
        offset += Consts.QuarterOfGB;
        return (accessor, prevAccessor, length, false);
    }
    catch (Exception)
    {
        return (mmf.CreateViewAccessor(offset, 0, MemoryMappedFileAccess.Read), prevAccessor, 0, true);
        //Console.WriteLine("Error: {0}", ex);
    }
}

(MemoryMappedViewAccessor accessor, int length, bool last) GetTheRestAccessor(MemoryMappedFile mmf, long offset, int goBack)
{
    var accessor = mmf.CreateViewAccessor(offset, 0);
    if (goBack == 0)
        return (accessor, 0, true);
    using (accessor)
    {
        var length = (int)accessor.SafeMemoryMappedViewHandle.ByteLength;
        Console.WriteLine("Bytes length: {0}", length);
        length += goBack;
        return TryFindAccessor(mmf, offset - goBack, length);
    }
}

(MemoryMappedViewAccessor accessor, int length, bool last) TryFindAccessor(MemoryMappedFile mmf, long offset, int length)
{
    (var accessor, int realLength) = GetSmaller(mmf, offset, length, 1024);
    return (accessor, realLength, true);
}

(MemoryMappedViewAccessor accessor, int length) GetSmaller(MemoryMappedFile mmf, long offset, int length, int jump)
{
    try
    {
        var accessor = mmf.CreateViewAccessor(offset, length - jump);
        if (jump == 1)
            return (accessor, length - 1);
        return GetBigger(mmf, offset, length, jump / 2);
    }
    catch
    {
        return GetSmaller(mmf, offset, length - jump, jump);
    }
}

(MemoryMappedViewAccessor accessor, int length) GetBigger(MemoryMappedFile mmf, long offset, int length, int jump)
{
    try
    {
        var accessor = mmf.CreateViewAccessor(offset, length + jump);
        if (jump == 1)
            return (accessor, length + 1);
        return GetBigger(mmf, offset, length + jump, jump);
    }
    catch
    {
        return GetSmaller(mmf, offset, length, jump / 2);
    }
}

unsafe int FindLastNewLine(MemoryMappedFile memoryMappedFile, long offset)
{
    const int searchSize = 100;
    if (offset == 0)
        return 0;
    using (var accessor = memoryMappedFile.CreateViewAccessor(offset - searchSize, searchSize))
    {
        byte* buffor = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref buffor);
        try
        {
            var byteSpan = new ReadOnlySpan<byte>(buffor, searchSize);
            var index = byteSpan.LastIndexOf((byte)'\n');
            if (index < 0)
                throw new Exception("Search size is too small");
            var goback = searchSize - index - 1;
            Console.WriteLine("Back: {0}", goback);
            return goback;
        }
        finally
        {
            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}

class Runner
{
    private readonly MemoryMappedViewAccessor accessor;
    private readonly MemoryMappedViewAccessor? prevAccessor;
    private readonly Dictionary<string, CityInfo> results;
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
        results = new Dictionary<string, CityInfo>();
        Awaitable = Task.Run(() => CountLines());
    }

    public Task Awaitable { get; }
    public int Result { get; private set; }
    public Dictionary<string, CityInfo> Results => results;

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
                byteSpan = byteSpan.Slice(index + 1);
                index = byteSpan.IndexOf((byte)'\n');
                //ReadOnlySpan<byte> lastLine = ReadOnlySpan<byte>.Empty;
                //ReadOnlySpan<byte> prevlastLine = ReadOnlySpan<byte>.Empty;

                while (index >= 0)
                {
                    count++;
                    var separatorIndex = byteSpan.IndexOf((byte)';');
                    var city = Encoding.UTF8.GetString(byteSpan.Slice(0, separatorIndex));
                    var value = double.Parse(byteSpan.Slice(separatorIndex + 1, index));
                    if (results.TryGetValue(city, out CityInfo? cityInfo))
                        cityInfo.Add(value);
                    else
                        results.Add(city, new CityInfo(value));
                    if (byteSpan.Length <= index + 1)
                        break;
                    //prevlastLine = lastLine;
                    //lastLine = byteSpan.Slice(0, index);
                    byteSpan = byteSpan.Slice(index + 1);
                    index = byteSpan.IndexOf((byte)'\n');
                }
                //Console.WriteLine(Encoding.UTF8.GetString(prevlastLine));
                //Console.WriteLine(Encoding.UTF8.GetString(lastLine));
                //var i = 0;
                //while (i < length && *buffor != 0)
                //{
                //    if ((char)*buffor == '\n')
                //    {
                //        count++;
                //    }
                //    buffor++;
                //    i++;
                //}
            }
            finally
            {
                accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
        Console.WriteLine(count);
        Result = count;
    }
}

class CityInfo
{
    public CityInfo(double value)
    {
        Min = Max = Sum = value;
        Count = 1;
    }
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
}

