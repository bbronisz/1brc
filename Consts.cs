internal static class Consts
{
    public static readonly int OneThreadBlockSize = Environment.ProcessorCount < 12 ? 0x20000000 : 0x1000000;//0x8000000; //128 MB;//0x600000;//0x10000000;// 256 megabytes
    public static readonly int MaxRunningThreads = 48;//Environment.ProcessorCount;
    public const int PrevAccessorLength = 64;
    public const int NewLineLength = 1;
}

internal static class Exts
{
    private const byte LF = (byte)'\n';

    public static int GetNewLineIndex(this Span<byte> bytes) =>
        bytes.IndexOf(LF);

    public static int GetLastNewLineIndex(this Span<byte> bytes) =>
        bytes.LastIndexOf(LF);
}