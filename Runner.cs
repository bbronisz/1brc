// See https://aka.ms/new-console-template for more information
using System.IO.MemoryMappedFiles;
using System.Text;

internal class Runner
{
    private readonly MemoryMappedViewAccessor accessor;
    private readonly MemoryMappedViewAccessor? prevAccessor;
    private readonly EventWaitHandle startFlag;
    private readonly Dictionary<int, CityInfo> results;
    private readonly int length;
    private readonly Thread thread;

    public Runner(MemoryMappedViewAccessor accessor, MemoryMappedViewAccessor? prevAccessor, int length, EventWaitHandle startFlag)
    {
        this.accessor = accessor;
        this.prevAccessor = prevAccessor;
        this.startFlag = startFlag;
        if (length > 0)
            this.length = length;
        else
        {
            this.length = (int)accessor.SafeMemoryMappedViewHandle.ByteLength;
            //Console.WriteLine("Byte length: {0}", this.length);
        }
        results = new Dictionary<int, CityInfo>(1024);

        thread = new Thread(ThreadMethod);
        try
        {
            thread.Priority = ThreadPriority.AboveNormal;
        }
        catch { }
        thread.Start();
    }

    public int Result { get; private set; }
    public Dictionary<int, CityInfo> Results => results;

    public void Join()
    {
        thread.Join();
    }

    private void ThreadMethod()
    {
        try
        {
            startFlag.WaitOne();
            ParseFile();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
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

    private static int GetHashCodeOG(Span<byte> citySpan)
    {
        //return city.Length;
        var hc = new HashCode();
        hc.AddBytes(citySpan);
        return hc.ToHashCode();
    }
    static readonly byte[] NormalizationArray =
        { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 0, 0, 0, 0, 0, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154 };

    private static int GetHashCode(Span<byte> citySpan)
    {
        var hc = 0;
        var index = 0;
        for (var i = 0; i < citySpan.Length; i++)
        {
            var b1 = NormalizationArray[citySpan[i]];
            if (b1 == 0)
                continue;
            hc ^= (b1 << index);
            index += 5;
        }
        return hc;
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

