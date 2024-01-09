// See https://aka.ms/new-console-template for more information
using System.IO.MemoryMappedFiles;
using System.Text;

class Runner
{
    //private static SemaphoreSlim Semaphore = new SemaphoreSlim(8);
    private readonly MemoryMappedViewAccessor accessor;
    private readonly MemoryMappedViewAccessor? prevAccessor;
    private readonly Dictionary<int, CityInfo> results;
    private readonly int length;
    private readonly Thread thread;
    private bool finished;

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
        thread = new Thread(ThreadMethod);
        thread.Priority = ThreadPriority.AboveNormal;
    }

    public int Result { get; private set; }
    public bool Finished => finished;
    public Dictionary<int, CityInfo> Results => results;

    public void Start(AutoResetEvent autoResetEvent) => thread.Start(autoResetEvent);

    private void ThreadMethod(object? state)
    {
        var autoResetEvent = state as AutoResetEvent;
        try
        {
            //Semaphore.Wait();
            ParseFile();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
        finally
        {
            Volatile.Write(ref finished, true);
            Interlocked.Decrement(ref Program.RunningThreads);
            autoResetEvent?.Set();
            //Semaphore.Release();
        }
    }

    private unsafe void ParseFile()
    {
        int count = 0;
        using (accessor)
        {
            byte* buffor = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref buffor);
            try
            {
                var byteSpan = new Span<byte>(buffor, length);
                var index = byteSpan.GetNewLineIndex();
                if (HandleSplitInTheMiddle(byteSpan, index))
                {
                    count++;
                    byteSpan = byteSpan.Slice(index + Consts.NewLineLength);
                    index = byteSpan.GetNewLineIndex();
                }

                while (index >= 0)
                {
                    count++;

                    AddCity(byteSpan.Slice(0, index));

                    if (byteSpan.Length <= index + Consts.NewLineLength)
                        break;
                    byteSpan = byteSpan.Slice(index + Consts.NewLineLength);
                    if (byteSpan[0] == '\0')
                        break;
                    index = byteSpan.GetNewLineIndex();
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

    private bool HandleSplitInTheMiddle(Span<byte> bytesSpan, int newLineIndex)
    {
        if (prevAccessor == null)
            return false;
        Span<byte> buffer = stackalloc byte[Consts.PrevAccessorLength];
        var prevSpan = GetPrev(prevAccessor, buffer);
        if (prevSpan.IsEmpty)
            return false;
        Span<byte> wholeLine = stackalloc byte[prevSpan.Length + newLineIndex];
        prevSpan.CopyTo(wholeLine);
        bytesSpan.Slice(0, newLineIndex).CopyTo(wholeLine.Slice(prevSpan.Length));

        AddCity(wholeLine);

        return true;
    }

    private void AddCity(Span<byte> line)
    {
        var separatorIndex = line.IndexOf((byte)';');
        if (separatorIndex < 0)
            throw new Exception(string.Format("Wrongly formatted line: {0}", Encoding.UTF8.GetString(line)));
        var citySpan = line.Slice(0, separatorIndex);
        var hashCode = GetHashCode(citySpan);
        var value = ParseNumber(line.Slice(separatorIndex + 1));
        if (results.TryGetValue(hashCode, out CityInfo? cityInfo))
            cityInfo.Add(value);
        else
            results.Add(hashCode, new CityInfo(Encoding.UTF8.GetString(citySpan), value));
    }

    private static int GetHashCode(Span<byte> citySpan)
    {
        //return city.Length;
        var hc = new HashCode();
        hc.AddBytes(citySpan);
        return hc.ToHashCode();
    }

    private static double ParseNumber(Span<byte> line)
    {
        var sign = 1d;
        if (line[0] == '-')
        {
            sign = -1d;
            line = line.Slice(1);
        }
        var dotIndex = line.IndexOf((byte)'.');
        if (dotIndex < 0)
        {
            return GetNatural(line) * sign;
        }
        double result = GetNatural(line.Slice(0, dotIndex));
        result += GetDecimal(line.Slice(dotIndex + 1));
        return sign * result;
    }

    private static int GetNatural(Span<byte> line)
    {
        var dec = 1;
        var result = 0;
        for (var i = line.Length - 1; i >= 0; i--)
        {
            result += (line[i] - '0') * dec;
            dec *= 10;
        }
        return result;
    }

    private static double GetDecimal(Span<byte> line)
    {
        if (line.IsEmpty)
            return 0d;
        var result = 0d;
        var dec = 0.1;
        foreach (var c in line)
        {
            result += (c - '0') * dec;
            dec /= 10;
        }
        return result;
    }

    private static unsafe Span<byte> GetPrev(MemoryMappedViewAccessor prevAccessor, Span<byte> buffer)
    {
        byte* bytes = null;
        prevAccessor.SafeMemoryMappedViewHandle.AcquirePointer(ref bytes);
        try
        {
            var localspan = new Span<byte>(bytes, (int)prevAccessor.SafeMemoryMappedViewHandle.ByteLength);
            //Console.WriteLine("len: {0}", localspan.Length);
            var index = localspan.GetLastNewLineIndex();
            if (index < 0 || index == localspan.Length - Consts.NewLineLength)
                return [];
            localspan = localspan.Slice(index + Consts.NewLineLength);
            localspan.CopyTo(buffer);
            return buffer.Slice(0, localspan.Length);
        }
        finally
        {
            prevAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }
}

