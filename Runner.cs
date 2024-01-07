// See https://aka.ms/new-console-template for more information
using System.IO.MemoryMappedFiles;
using System.Text;

class Runner
{
    private readonly MemoryMappedViewAccessor accessor;
    private readonly MemoryMappedViewAccessor? prevAccessor;
    private readonly Dictionary<int, CityInfo> results;
    private readonly int length;
    private readonly TaskCompletionSource taskSource;

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
        taskSource = new TaskCompletionSource();
        Awaitable = taskSource.Task;
        new Thread(ThreadMethod).Start();
    }

    public Task Awaitable { get; }
    public int Result { get; private set; }
    public Dictionary<int, CityInfo> Results => results;

    private void ThreadMethod()
    {
        try
        {
            ParseFile();
        }
        catch (Exception ex)
        {
            taskSource.SetException(ex);
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
                var byteSpan = new ReadOnlySpan<byte>(buffor, length);
                var index = byteSpan.GetNewLineIndex();
                if (HandleSplitInTheMiddle(byteSpan, index))
                {
                    byteSpan = byteSpan.Slice(index + 1);
                    index = byteSpan.GetNewLineIndex();
                }

                while (index >= 0)
                {
                    count++;

                    AddCity(byteSpan.Slice(0, index));

                    if (byteSpan.Length <= index + 1)
                        break;
                    byteSpan = byteSpan.Slice(index + 1);
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
        taskSource.SetResult();
    }

    private bool HandleSplitInTheMiddle(ReadOnlySpan<byte> bytesSpan, int newLineIndex)
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

    private void AddCity(ReadOnlySpan<byte> line)
    {
        var separatorIndex = line.IndexOf((byte)';');
        var citySpan = line.Slice(0, separatorIndex);
        var hc = new HashCode();
        hc.AddBytes(citySpan);
        var hashCode = hc.ToHashCode();
        //var hashCode = (int)byteSpan[0];
        var value = 0d;//double.Parse(byteSpan.Slice(separatorIndex + 1, index - separatorIndex - 1));
        if (results.TryGetValue(hashCode, out CityInfo? cityInfo))
            cityInfo.Add(value);
        else
            results.Add(hashCode, new CityInfo(Encoding.UTF8.GetString(citySpan), value));
    }

    private static unsafe ReadOnlySpan<byte> GetPrev(MemoryMappedViewAccessor prevAccessor, Span<byte> buffer)
    {
        prevAccessor.SafeMemoryMappedViewHandle.ReadSpan(0, buffer);
        var index = buffer.GetNewLineIndex();
        if (index < 0 || index == buffer.Length - Consts.NewLineLength)
            return [];
        return buffer.Slice(index + Consts.NewLineLength);
    }
}

